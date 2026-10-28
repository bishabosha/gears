// What Network.framework's C interface gives for QUIC (see "QUIC and TLS from Network.framework itself" in
// docs/io/missing-api.md): a listener whose connections come as connection groups, a client group (multiplex
// descriptor), streams extracted from a group and accepted from it, each an nw_connection_t, and how a stream's data,
// its end, its id and its type look. Prints what happens; nothing asserted. Over loopback, port 47712.
//
// The server needs a TLS identity, imported into memory only (kSecImportToMemoryOnly), not the keychain:
//
//   openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -keyout key.pem -out cert.pem -days 2 \
//     -subj /CN=localhost -addext subjectAltName=IP:127.0.0.1,DNS:localhost
//   openssl pkcs12 -export -out id.p12 -inkey key.pem -in cert.pem -passout pass:gears \
//     -keypbe PBE-SHA1-3DES -certpbe PBE-SHA1-3DES -macalg sha1
//   clang -O1 -o quic network-framework-quic.c -framework Network -framework Security -framework CoreFoundation
//   ./quic                 # in the directory holding id.p12; EXPLICIT=1 gives the server's stream explicit options
#include <Network/Network.h>
#include <Security/Security.h>
#include <dispatch/dispatch.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static dispatch_queue_t q;
static sec_identity_t identity;
static nw_protocol_definition_t quic;

static void say(const char *who, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  printf("%-7s ", who);
  vprintf(fmt, ap);
  printf("\n");
  va_end(ap);
}

static sec_identity_t load_identity(const char *path) {
  CFDataRef data = NULL;
  FILE *f = fopen(path, "rb");
  char buf[8192];
  size_t n = fread(buf, 1, sizeof buf, f);
  fclose(f);
  data = CFDataCreate(NULL, (const UInt8 *)buf, n);
  const void *keys[] = {kSecImportExportPassphrase, kSecImportToMemoryOnly};
  const void *values[] = {CFSTR("gears"), kCFBooleanTrue};
  CFDictionaryRef options = CFDictionaryCreate(NULL, keys, values, 2, NULL, NULL);
  CFArrayRef items = NULL;
  OSStatus status = SecPKCS12Import(data, options, &items);
  if (status != errSecSuccess) {
    printf("SecPKCS12Import failed: %d\n", (int)status);
    exit(1);
  }
  SecIdentityRef id = (SecIdentityRef)CFDictionaryGetValue(CFArrayGetValueAtIndex(items, 0), kSecImportItemIdentity);
  return sec_identity_create(id);
}

static nw_parameters_t quic_parameters(int server) {
  return nw_parameters_create_quic(^(nw_protocol_options_t options) {
    nw_quic_add_tls_application_protocol(options, "gears");
    nw_quic_set_initial_max_streams_bidirectional(options, 16);
    nw_quic_set_initial_max_streams_unidirectional(options, 16);
    nw_quic_set_max_datagram_frame_size(options, 1200);
    nw_quic_set_idle_timeout(options, 10000);
    sec_protocol_options_t tls = nw_quic_copy_sec_protocol_options(options);
    if (server) sec_protocol_options_set_local_identity(tls, identity);
    else
      sec_protocol_options_set_verify_block(
          tls, ^(sec_protocol_metadata_t m, sec_trust_t t, sec_protocol_verify_complete_t complete) { complete(true); },
          q);
  });
}

static const char *type_name(nw_connection_t c) {
  nw_protocol_metadata_t m = nw_connection_copy_protocol_metadata(c, quic);
  if (!m) return "no quic metadata";
  static char text[64];
  int t = nw_quic_get_stream_type(m);
  snprintf(text, sizeof text, "id %llu, %s", (unsigned long long)nw_quic_get_stream_id(m),
           t == 1 ? "bidirectional" : t == 2 ? "unidirectional" : t == 3 ? "datagram" : "unknown type");
  return text;
}

/* Reads a stream to its end (or one message), printing what comes, then calls `done` with the text. */
static void read_all(const char *who, nw_connection_t c, int message, void (^done)(const char *text)) {
  __block char *text = calloc(1, 65536);
  __block size_t len = 0;
  __block void (^next)(void);
  next = ^{
    void (^handler)(dispatch_data_t, nw_content_context_t, bool, nw_error_t) =
        ^(dispatch_data_t content, nw_content_context_t context, bool complete, nw_error_t error) {
          if (content) {
            dispatch_data_apply(content, ^bool(dispatch_data_t r, size_t o, const void *b, size_t l) {
              memcpy(text + len, b, l);
              len += l;
              return true;
            });
          }
          int final = context && nw_content_context_get_is_final(context);
          if (error) say(who, "receive error %d", nw_error_get_error_code(error));
          if (complete || error || message) {
            say(who, "read \"%.*s\" (complete %d, final context %d) on %s", (int)len, text, complete, final,
                type_name(c));
            done(text);
          } else next();
        };
    if (message) nw_connection_receive_message(c, handler);
    else nw_connection_receive(c, 1, 65536, handler);
  };
  next();
}

static void send_final(nw_connection_t c, const char *text) {
  dispatch_data_t data = dispatch_data_create(text, strlen(text), q, DISPATCH_DATA_DESTRUCTOR_DEFAULT);
  nw_connection_send(c, data, NW_CONNECTION_FINAL_MESSAGE_CONTEXT, true, ^(nw_error_t e) {});
}

static void send_message(nw_connection_t c, const char *text) {
  dispatch_data_t data = dispatch_data_create(text, strlen(text), q, DISPATCH_DATA_DESTRUCTOR_DEFAULT);
  nw_connection_send(c, data, NW_CONNECTION_DEFAULT_MESSAGE_CONTEXT, true, ^(nw_error_t e) {});
}

static void start(const char *who, nw_connection_t c, void (^ready)(void)) {
  nw_connection_set_queue(c, q);
  nw_connection_set_state_changed_handler(c, ^(nw_connection_state_t s, nw_error_t e) {
    if (s == nw_connection_state_ready) ready();
    else if (s == nw_connection_state_failed) say(who, "stream failed %d", e ? nw_error_get_error_code(e) : 0);
  });
  nw_connection_start(c);
}

int main(void) {
  setvbuf(stdout, NULL, _IONBF, 0);
  q = dispatch_queue_create("probe", NULL);
  quic = nw_protocol_copy_quic_definition();
  identity = load_identity("id.p12");
  dispatch_semaphore_t finished = dispatch_semaphore_create(0);

  // ---- server: a listener whose QUIC connections come as connection groups ----
  nw_parameters_t sp = quic_parameters(1);
  nw_parameters_set_local_endpoint(sp, nw_endpoint_create_host("127.0.0.1", "47712"));
  nw_listener_t listener = nw_listener_create(sp);
  nw_listener_set_queue(listener, q);
  __block nw_connection_group_t serverGroup = NULL;
  nw_listener_set_new_connection_group_handler(listener, ^(nw_connection_group_t group) {
    say("server", "new connection group");
    serverGroup = group;
    nw_connection_group_set_queue(group, q);
    nw_connection_group_set_new_connection_handler(group, ^(nw_connection_t stream) {
      start("server", stream, ^{
        say("server", "inbound stream ready: %s", type_name(stream));
        nw_protocol_metadata_t m = nw_connection_copy_protocol_metadata(stream, quic);
        int t = m ? nw_quic_get_stream_type(m) : 0;
        if (t == 3) { // datagram flow: echo each message
          read_all("server", stream, 1, ^(const char *text) { send_message(stream, "datagram back"); });
        } else if (t == 2) {
          read_all("server", stream, 0, ^(const char *text) {});
        } else {
          read_all("server", stream, 0, ^(const char *text) {
            char reply[256];
            snprintf(reply, sizeof reply, "echo: %s", text);
            send_final(stream, reply);
          });
        }
      });
    });
    nw_connection_group_set_state_changed_handler(group, ^(nw_connection_group_state_t s, nw_error_t e) {
      say("server", "group state %d", s);
      if (s == nw_connection_group_state_ready) {
        // A stream the server opens, extracted from the group.
        nw_protocol_options_t bidi = nw_quic_create_options(); // a plain stream, said so
        nw_connection_t own = nw_connection_group_extract_connection(group, NULL, getenv("EXPLICIT") ? bidi : NULL);
        if (!own) say("server", "extract returned NULL");
        else
          start("server", own, ^{
            say("server", "own stream ready: %s", type_name(own));
            send_final(own, "from server");
          });
      }
    });
    nw_connection_group_start(group);
  });
  nw_listener_set_state_changed_handler(listener, ^(nw_listener_state_t s, nw_error_t e) {
    say("server", "listener state %d%s", s, s == nw_listener_state_ready ? " (ready)" : "");
  });
  nw_listener_start(listener);
  sleep(1);

  // ---- client: a multiplex group, streams extracted from it ----
  nw_endpoint_t server = nw_endpoint_create_host("127.0.0.1", "47712");
  nw_connection_group_t group = nw_connection_group_create(nw_group_descriptor_create_multiplex(server), quic_parameters(0));
  nw_connection_group_set_queue(group, q);
  nw_connection_group_set_new_connection_handler(group, ^(nw_connection_t stream) {
    start("client", stream, ^{
      say("client", "inbound stream ready: %s", type_name(stream));
      read_all("client", stream, 0, ^(const char *text) {});
    });
  });
  __block int echoes = 0;
  nw_connection_group_set_state_changed_handler(group, ^(nw_connection_group_state_t s, nw_error_t e) {
    say("client", "group state %d%s", s, s == nw_connection_group_state_ready ? " (ready)" : "");
    if (s == nw_connection_group_state_failed) {
      say("client", "group failed: %d", e ? nw_error_get_error_code(e) : 0);
      dispatch_semaphore_signal(finished);
    }
    if (s != nw_connection_group_state_ready) return;
    for (int i = 0; i < 3; i++) {
      nw_connection_t stream = nw_connection_group_extract_connection(group, NULL, NULL);
      char *text = malloc(32);
      snprintf(text, 32, "stream %d", i);
      start("client", stream, ^{
        say("client", "own stream ready: %s", type_name(stream));
        send_final(stream, text);
        read_all("client", stream, 0, ^(const char *reply) {
          if (++echoes == 3) dispatch_semaphore_signal(finished);
        });
      });
    }
    // A unidirectional stream, and a datagram flow, each asked for by the options given to extract.
    nw_protocol_options_t uni = nw_quic_create_options();
    nw_quic_set_stream_is_unidirectional(uni, true);
    nw_connection_t one = nw_connection_group_extract_connection(group, NULL, uni);
    start("client", one, ^{
      say("client", "unidirectional stream ready: %s", type_name(one));
      send_final(one, "one way");
    });
    nw_protocol_options_t dgram = nw_quic_create_options();
    nw_quic_set_stream_is_datagram(dgram, true);
    nw_connection_t flow = nw_connection_group_extract_connection(group, NULL, dgram);
    start("client", flow, ^{
      say("client", "datagram flow ready: %s", type_name(flow));
      send_message(flow, "a datagram");
      read_all("client", flow, 1, ^(const char *text) {});
    });
  });
  nw_connection_group_start(group);

  long r = dispatch_semaphore_wait(finished, dispatch_time(DISPATCH_TIME_NOW, 8LL * NSEC_PER_SEC));
  sleep(1); // let the other prints land
  say("main", r == 0 ? "three echoes back" : "timed out waiting for three echoes");
  nw_connection_group_cancel(group);
  nw_listener_cancel(listener);
  sleep(1);
  return 0;
}
