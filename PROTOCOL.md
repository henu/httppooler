HttpPooler wire protocol
========================

Version 0. This is everything a client and the server put on one TCP connection, from the first byte.
Integers are little-endian, fixed width, no padding. `strN` is a uN byte count followed by that many bytes
and no terminator; the count is bytes, never characters. For names the bytes are UTF-8, for HTTP header
values they are whatever HTTP allows. Parsing is strictly sequential: a reader consumes every byte of every
item and never skips. Anything malformed — wrong magic, unknown kind, a kind the direction does not allow, a
count outside its stated range, a failed authentication tag — refuses the connection whole with a log line;
nothing is recovered.

There are no size caps. The only bounds are structural (a u16 length is at most 65535) or inherited (a Noise
message is at most 65535 bytes); bodies of any size travel as chunks.

There is no per-job window either. A peer that takes a body in faster than it can pass it on holds the
difference in memory: a provider's slots bound how many jobs run at once, and nothing bounds that. Flow
control would be a new version.

Terms
-----

- server: the peer that listens. It holds the pool, i.e. the queues and the slot accounting.
- client: a peer that dials the server and keeps the connection open.
- provider: a peer's `[provide]` section, an upstream that executes jobs for one service with a fixed
  number of slots.
- consumer: a peer's `[consume]` section, a local port where applications submit requests for one service.
- consumer side and provider side: the two ends of one hop. The consumer side sends REQUEST, the provider
  side answers it. Between a client's consumer and the server, the server is the provider side; between the
  server and a client's provider, the server is the consumer side. The server is a relay, both cases run on
  the same connection, and the messages are the same in both directions. Below, "consumer→provider" means
  from the consumer side of the hop to the provider side, whichever peers those happen to be.

Layer 0: preamble
-----------------

The initiator sends five bytes immediately after connecting, without waiting for the other side; the
responder reads them first and sends its own with its handshake message, as layer 1 says:

    offset  size  value
    0       4     magic "HPOL" (0x48 0x50 0x4F 0x4C)
    4       1     version u8: VERSION_0_INITIAL = 0; VERSION_NEWEST = VERSION_0_INITIAL

Versions count from 0 and are named for what they introduced; the next would be VERSION_1_<FEATURE>. In Go
the same constants are spelled Version0Initial and VersionNewest. Any layout change, a new message kind
included, is a new version.

Each side reads the other's five bytes. Wrong magic: close silently (port scanners). Different version: log
both versions and close. A later version may choose to accept older peers; version 0 requires equality.

Layer 1: handshake
------------------

Noise_NNpsk0_25519_ChaChaPoly_SHA256, exactly as the Noise specification (revision 34) composes it, with

- prologue = the five preamble bytes,
- psk = the 32-byte shared secret from the conf,
- the client as initiator and the server as responder.

Each handshake message is framed as u16 length + bytes. The initiator sends its message right after its
preamble without waiting; the responder answers preamble + message together, so the whole setup is one round
trip.

    -> psk, e     client→server   len 48: e.pub (32) + encrypted empty payload (16, tag only)
    <- e, ee      server→client   len 48: e.pub (32) + encrypted empty payload (16, tag only)

Steps, in the specification's vocabulary (h, ck, k, n):

1. h = SHA256("Noise_NNpsk0_25519_ChaChaPoly_SHA256"), because the name is longer than 32 bytes; ck = h.
2. MixHash(prologue).
3. Initiator: MixKeyAndHash(psk); e = fresh X25519 keypair; write e.pub; MixHash(e.pub); MixKey(e.pub);
   write EncryptAndHash(empty).
4. Responder: MixKeyAndHash(psk); read re; MixHash(re); MixKey(re); DecryptAndHash(16 bytes) — a wrong
   secret fails here. Then e = fresh keypair; write e.pub; MixHash(e.pub); MixKey(e.pub); MixKey(DH(e, re));
   write EncryptAndHash(empty).
5. Initiator: read re; MixHash(re); MixKey(re); MixKey(DH(e, re)); DecryptAndHash(16 bytes).
6. Split(): HKDF(ck, empty, 2) gives k1 for client→server and k2 for server→client; both counters start
   at 0.

Noise's HKDF is: temp = HMAC-SHA256(ck, ikm); out1 = HMAC(temp, 0x01); out2 = HMAC(temp, out1 ‖ 0x02);
out3 = HMAC(temp, out2 ‖ 0x03). Go's crypto/hkdf Extract + Expand with empty info produces exactly this.
The ChaCha20-Poly1305 nonce is four zero bytes followed by n as u64 little-endian; n increments per message
per direction and is never reused. That is the replay and reordering defence; no extra sequence field.

MixKey(e.pub) after every e is the psk-mode rule (specification section 9), not a typo.

Correctness is shown by the published test vectors for this exact suite (from the flynn/noise vectors.txt,
vendored into the test data) and by a test that completes the handshake against flynn/noise in both roles.

Layer 2: records
----------------

After the handshake, each direction is a stream of records:

    offset  size  value
    0       2     len u16: 17..65535
    2       len   ciphertext = ChaCha20-Poly1305(plaintext) + 16-byte tag, AD empty, nonce = counter

The plaintexts, concatenated, form one byte stream per direction. Record boundaries carry no meaning: a
message may span records, and one record may carry several messages. Senders size records as they like up
to the maximum plaintext of 65519 bytes. An empty plaintext is not allowed, hence the minimum length 17.

Layer 3: messages
-----------------

Parsed sequentially from the plaintext stream. Every message begins with a kind byte and continues with the
fields of that kind only. "header" below means `name str8` followed by `value str16`.

`job` is a u64 chosen by the consumer side, never reused on the connection, never 0. Jobs the server starts
get even ids (2, 4, 6, …), jobs a client starts odd ones (1, 3, 5, …). The parity tells each side which side
of that job it is, so no message needs a direction flag.

HELLO (0x01), client→server, the client's first message, exactly once

    kind            u8      0x01
    name            str8    peer name from conf, default hostname
    count           u8      providers on this client; 0 for a consumer-only client
    count × provider
      service       str8    service name, the same string as in [provide "…"] and [consume "…"]
      max_concurrent u16    slots: jobs this upstream runs at the same time, >= 1
      priority      u16     when several providers have a free slot, the smallest number is chosen

One provider per service per peer: a REQUEST names the service and nothing else, so a peer announcing the
same service twice would be a job with two places to go and no way to say which. The server refuses such a
HELLO and closes the connection.

PING (0x02) and PONG (0x03), both directions, no fields.

REQUEST (0x10), consumer→provider

    kind            u8      0x10
    job             u64
    service         str8    the pool to queue in (client→server) or the [provide] to use (server→client)
    method          str8    HTTP method as written
    target          str16   request-target from the request line: path and query
    hcount          u16
    hcount × header         see the header rules below

RESPONSE (0x11), provider→consumer

    kind            u8      0x11
    job             u64
    status          u16     100..599
    hcount          u16
    hcount × header

BODY (0x12), both directions: the request body consumer→provider, the response body provider→consumer

    kind            u8      0x12
    job             u64
    len             u32     >= 1
    bytes           len     a piece of the body, in order

END (0x13), both directions; ends the body of that job in that direction

    kind            u8      0x13
    job             u64
    tcount          u16     trailers, normally 0
    tcount × header

CANCEL (0x14), consumer→provider

    kind            u8      0x14
    job             u64

FAIL (0x15), provider→consumer; terminates the job abnormally

    kind            u8      0x15
    job             u64
    reason          str16   human-readable, ends up in the log and in the 502 body

Header rules: hop-by-hop headers (Connection, Keep-Alive, Transfer-Encoding, TE, Trailer, Upgrade,
Proxy-Connection) are never sent. Host is not sent; the provider sets it to its upstream address, as any
reverse proxy does. Content-Length is sent when known. Everything else passes through unchanged and in
order.

A job's message sequence
------------------------

    consumer→provider  REQUEST, BODY*, END
    provider→consumer  RESPONSE, BODY*, END      success
                   or  FAIL                      before RESPONSE: no HTTP response exists; the consumer
                                                 answers its application 502 with the reason
                   or  RESPONSE, BODY*, FAIL     after RESPONSE: the body died midway; the consumer aborts
                                                 the application's connection

- A consumer reads the whole request body before sending REQUEST, so the server can hand the job to another
  provider if one vanishes. A request without body is REQUEST, END.
- A request that asks to become something else, a WebSocket or any other Upgrade, never becomes a job. The
  consumer side answers it 501 where it arrives, since nothing here could carry a connection.
- The server is the provider side toward a submitting client and the consumer side toward the chosen
  provider. It relays RESPONSE, BODY, END and FAIL from the provider to the submitter, and BODY, END and
  CANCEL the other way, renumbering the job for the second connection. Pool-level failures it answers itself
  with a RESPONSE: 503 when no provider frees a slot within queue_timeout, 502 with the reason when the
  provider sent FAIL before RESPONSE. An application therefore sees the same statuses whichever peer its
  consumer sits on.
- The provider sends RESPONSE as soon as the upstream's headers arrive and each BODY as the bytes arrive;
  every hop passes each BODY on at once. That is all streaming needs.
- CANCEL means the application went away. The provider side aborts the job and finishes its sequence with
  FAIL (reason "cancelled") unless END was already sent; the server relays a CANCEL onward. The consumer
  side ignores further messages for a cancelled job until its terminating END or FAIL, then forgets the id.
- A provider slot is busy from REQUEST until the job's terminating END or FAIL. A provider receiving more
  concurrent jobs than it advertised slots for treats that as a protocol error.
- Messages of concurrent jobs interleave freely on one connection; a BODY is written as one unit.
- Connection loss. Jobs handed to that client's providers that had not yet produced RESPONSE go back to the
  front of their queue; the queue_timeout deadline counts from the job's original arrival, not per attempt.
  Jobs handed to it that were mid-response are terminated toward their consumer with FAIL, or a connection
  abort for a local consumer. Jobs that client's consumers had submitted are dropped: cancelled at their
  provider if already running.

Liveness
--------

The server sends PING every 30 s. Any received message counts as life; a side that receives nothing for
90 s closes the connection. Clients reconnect with exponential backoff, 1 s doubling to 60 s, with jitter.
TCP keepalive is on as well.

In-process peers
----------------

The server's own provide and consume sections run the same code over an in-memory connection: layer 3
only, with no preamble, handshake or records. Same messages, same semantics, same code.
