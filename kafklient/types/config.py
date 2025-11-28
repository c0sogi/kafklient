from typing import Literal, TypedDict

# =============================================================================
# librdkafka Configuration TypedDict Definitions
# Reference: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
# Note: Using functional TypedDict syntax to support keys with dots (.)
# =============================================================================

# Common configuration properties applicable to both Consumer and Producer
CommonConfig = TypedDict(
    "CommonConfig",
    {
        # Basic Configuration
        "builtin.features": str,  # Builtin features for this build. Type: CSV flags
        "client.id": str,  # Client identifier. Default: rdkafka
        "metadata.broker.list": str,  # Initial list of brokers as CSV
        "bootstrap.servers": str,  # Alias for metadata.broker.list
        # Message Size Configuration
        "message.max.bytes": int,  # Max request message size. Range: 1000-1000000000. Default: 1000000
        "message.copy.max.bytes": int,  # Max size for message copy. Range: 0-1000000000. Default: 65535
        "receive.message.max.bytes": int,  # Max response message size. Range: 1000-2147483647. Default: 100000000
        # In-Flight Requests
        "max.in.flight.requests.per.connection": int,  # Max in-flight requests. Range: 1-1000000. Default: 1000000
        "max.in.flight": int,  # Alias for max.in.flight.requests.per.connection
        # Metadata Configuration
        "metadata.recovery.strategy": Literal[
            "none", "rebootstrap"
        ],  # Recovery strategy. Default: rebootstrap
        "metadata.recovery.rebootstrap.trigger.ms": int,  # Rebootstrap trigger. Range: 0-2147483647. Default: 300000
        "topic.metadata.refresh.interval.ms": int,  # Metadata refresh interval. Range: -1-3600000. Default: 300000
        "metadata.max.age.ms": int,  # Metadata cache max age. Range: 1-86400000. Default: 900000
        "topic.metadata.refresh.fast.interval.ms": int,  # Fast refresh interval. Range: 1-60000. Default: 100
        "topic.metadata.refresh.sparse": bool,  # Sparse metadata requests. Default: true
        "topic.metadata.propagation.max.ms": int,  # Topic propagation max time. Range: 0-3600000. Default: 30000
        "topic.blacklist": str,  # Topic blacklist regex patterns
        # Debug
        "debug": str,  # Debug contexts: generic, broker, topic, metadata, feature, queue, msg, protocol, cgrp, security, fetch, interceptor, plugin, consumer, admin, eos, mock, assignor, conf, telemetry, all
        # Socket Configuration
        "socket.timeout.ms": int,  # Network request timeout. Range: 10-300000. Default: 60000
        "socket.send.buffer.bytes": int,  # Send buffer size. Range: 0-100000000. Default: 0
        "socket.receive.buffer.bytes": int,  # Receive buffer size. Range: 0-100000000. Default: 0
        "socket.keepalive.enable": bool,  # Enable TCP keep-alives. Default: false
        "socket.nagle.disable": bool,  # Disable Nagle algorithm. Default: true
        "socket.max.fails": int,  # Max send failures before disconnect. Range: 0-1000000. Default: 1
        "socket.connection.setup.timeout.ms": int,  # Connection setup timeout. Range: 1000-2147483647. Default: 30000
        # Broker Configuration
        "broker.address.ttl": int,  # Address cache TTL. Range: 0-86400000. Default: 1000
        "broker.address.family": Literal[
            "any", "v4", "v6"
        ],  # IP address family. Default: any
        # Connection Configuration
        "connections.max.idle.ms": int,  # Close idle connections. Range: 0-2147483647. Default: 0
        "reconnect.backoff.ms": int,  # Initial reconnect backoff. Range: 0-3600000. Default: 100
        "reconnect.backoff.max.ms": int,  # Max reconnect backoff. Range: 0-3600000. Default: 10000
        # Retry Configuration
        "retry.backoff.ms": int,  # Retry backoff. Range: 1-300000. Default: 100
        "retry.backoff.max.ms": int,  # Max retry backoff. Range: 1-300000. Default: 1000
        # Statistics
        "statistics.interval.ms": int,  # Statistics emit interval. Range: 0-86400000. Default: 0
        "enabled_events": int,  # rd_kafka_conf_set_events(). Range: 0-2147483647. Default: 0
        # Logging
        "log_level": int,  # Logging level (syslog). Range: 0-7. Default: 6
        "log.queue": bool,  # Disable spontaneous log_cb. Default: false
        "log.thread.name": bool,  # Print thread name in logs. Default: true
        "log.connection.close": bool,  # Log broker disconnects. Default: true
        # Misc
        "enable.random.seed": bool,  # Initialize PRNG with srand(). Default: true
        "internal.termination.signal": int,  # Quick termination signal. Range: 0-128. Default: 0
        "api.version.request.timeout.ms": int,  # API version request timeout. Range: 1-300000. Default: 10000
        "allow.auto.create.topics": bool,  # Allow auto topic creation. Default: false
        # Security Protocol
        "security.protocol": Literal[
            "plaintext", "ssl", "sasl_plaintext", "sasl_ssl"
        ],  # Protocol. Default: plaintext
        # SSL Configuration
        "ssl.cipher.suites": str,  # Cipher suites for TLS/SSL
        "ssl.curves.list": str,  # Supported curves for TLS ClientHello
        "ssl.sigalgs.list": str,  # Signature algorithms for TLS ClientHello
        "ssl.key.location": str,  # Path to client's private key (PEM)
        "ssl.key.password": str,  # Private key passphrase
        "ssl.key.pem": str,  # Client's private key string (PEM)
        "ssl.certificate.location": str,  # Path to client's public key (PEM)
        "ssl.certificate.pem": str,  # Client's public key string (PEM)
        "ssl.ca.location": str,  # Path to CA certificate(s)
        "ssl.ca.pem": str,  # CA certificate string (PEM)
        "ssl.ca.certificate.stores": str,  # Windows Certificate stores. Default: Root
        "ssl.crl.location": str,  # Path to CRL
        "ssl.keystore.location": str,  # Path to keystore (PKCS#12)
        "ssl.keystore.password": str,  # Keystore password
        "ssl.providers": str,  # OpenSSL 3.0.x providers
        "ssl.engine.id": str,  # OpenSSL engine id. Default: dynamic
        "enable.ssl.certificate.verification": bool,  # Enable broker cert verification. Default: true
        "ssl.endpoint.identification.algorithm": Literal[
            "none", "https"
        ],  # Endpoint identification. Default: https
        # HTTPS Configuration (for OIDC)
        "https.ca.location": str,  # Path to CA for HTTPS endpoints
        "https.ca.pem": str,  # CA certificate for HTTPS endpoints
        # SASL Configuration
        "sasl.mechanisms": str,  # SASL mechanism: GSSAPI, PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER. Default: GSSAPI
        "sasl.mechanism": str,  # Alias for sasl.mechanisms
        "sasl.kerberos.service.name": str,  # Kerberos principal. Default: kafka
        "sasl.kerberos.principal": str,  # Client's Kerberos principal. Default: kafkaclient
        "sasl.kerberos.kinit.cmd": str,  # Kerberos kinit command
        "sasl.kerberos.keytab": str,  # Path to Kerberos keytab
        "sasl.kerberos.min.time.before.relogin": int,  # Key refresh interval. Range: 0-86400000. Default: 60000
        "sasl.username": str,  # SASL username for PLAIN/SCRAM
        "sasl.password": str,  # SASL password for PLAIN/SCRAM
        # OAUTHBEARER Configuration
        "sasl.oauthbearer.config": str,  # OAUTHBEARER configuration
        "enable.sasl.oauthbearer.unsecure.jwt": bool,  # Enable unsecure JWT handler. Default: false
        "sasl.oauthbearer.method": Literal[
            "default", "oidc"
        ],  # Login method. Default: default
        "sasl.oauthbearer.client.id": str,  # Public identifier (OIDC)
        "sasl.oauthbearer.client.credentials.client.id": str,  # Alias for client.id
        "sasl.oauthbearer.client.secret": str,  # Client secret (OIDC)
        "sasl.oauthbearer.client.credentials.client.secret": str,  # Alias for client.secret
        "sasl.oauthbearer.scope": str,  # Access request scope (OIDC)
        "sasl.oauthbearer.extensions": str,  # Additional info for broker (OIDC)
        "sasl.oauthbearer.token.endpoint.url": str,  # Token endpoint URL (OIDC)
        "sasl.oauthbearer.grant.type": Literal[
            "client_credentials", "urn:ietf:params:oauth:grant-type:jwt-bearer"
        ],  # Grant type. Default: client_credentials
        "sasl.oauthbearer.assertion.algorithm": Literal[
            "RS256", "ES256"
        ],  # JWT assertion algorithm. Default: RS256
        "sasl.oauthbearer.assertion.private.key.file": str,  # Private key for JWT assertion
        "sasl.oauthbearer.assertion.private.key.passphrase": str,  # Key passphrase
        "sasl.oauthbearer.assertion.private.key.pem": str,  # Private key (PEM)
        "sasl.oauthbearer.assertion.file": str,  # Assertion file path
        "sasl.oauthbearer.assertion.claim.aud": str,  # JWT audience claim
        "sasl.oauthbearer.assertion.claim.exp.seconds": int,  # Assertion expiration. Range: 1-2147483647. Default: 300
        "sasl.oauthbearer.assertion.claim.iss": str,  # JWT issuer claim
        "sasl.oauthbearer.assertion.claim.jti.include": bool,  # Include JWT ID. Default: false
        "sasl.oauthbearer.assertion.claim.nbf.seconds": int,  # Assertion not-before. Range: 0-2147483647. Default: 60
        "sasl.oauthbearer.assertion.claim.sub": str,  # JWT subject claim
        "sasl.oauthbearer.assertion.jwt.template.file": str,  # JWT template file
        "sasl.oauthbearer.metadata.authentication.type": Literal[
            "none", "azure_imds"
        ],  # Metadata auth type. Default: none
        # Plugin Configuration
        "plugin.library.paths": str,  # Plugin libraries (; separated)
        # Client Rack
        "client.rack": str,  # Rack identifier
        # Metrics
        "enable.metrics.push": bool,  # Enable metrics push. Default: true
        # DNS
        "client.dns.lookup": Literal[
            "use_all_dns_ips", "resolve_canonical_bootstrap_servers_only"
        ],  # DNS lookup. Default: use_all_dns_ips
    },
    total=False,
)

# Consumer configuration properties (extends CommonConfig conceptually)
ConsumerConfig = TypedDict(
    "ConsumerConfig",
    {
        # === All CommonConfig properties ===
        # Basic Configuration
        "builtin.features": str,
        "client.id": str,
        "metadata.broker.list": str,
        "bootstrap.servers": str,
        # Message Size Configuration
        "message.max.bytes": int,
        "message.copy.max.bytes": int,
        "receive.message.max.bytes": int,
        # In-Flight Requests
        "max.in.flight.requests.per.connection": int,
        "max.in.flight": int,
        # Metadata Configuration
        "metadata.recovery.strategy": Literal["none", "rebootstrap"],
        "metadata.recovery.rebootstrap.trigger.ms": int,
        "topic.metadata.refresh.interval.ms": int,
        "metadata.max.age.ms": int,
        "topic.metadata.refresh.fast.interval.ms": int,
        "topic.metadata.refresh.sparse": bool,
        "topic.metadata.propagation.max.ms": int,
        "topic.blacklist": str,
        # Debug
        "debug": str,
        # Socket Configuration
        "socket.timeout.ms": int,
        "socket.send.buffer.bytes": int,
        "socket.receive.buffer.bytes": int,
        "socket.keepalive.enable": bool,
        "socket.nagle.disable": bool,
        "socket.max.fails": int,
        "socket.connection.setup.timeout.ms": int,
        # Broker Configuration
        "broker.address.ttl": int,
        "broker.address.family": Literal["any", "v4", "v6"],
        # Connection Configuration
        "connections.max.idle.ms": int,
        "reconnect.backoff.ms": int,
        "reconnect.backoff.max.ms": int,
        # Retry Configuration
        "retry.backoff.ms": int,
        "retry.backoff.max.ms": int,
        # Statistics
        "statistics.interval.ms": int,
        "enabled_events": int,
        # Logging
        "log_level": int,
        "log.queue": bool,
        "log.thread.name": bool,
        "log.connection.close": bool,
        # Misc
        "enable.random.seed": bool,
        "internal.termination.signal": int,
        "api.version.request.timeout.ms": int,
        "allow.auto.create.topics": bool,
        # Security Protocol
        "security.protocol": Literal["plaintext", "ssl", "sasl_plaintext", "sasl_ssl"],
        # SSL Configuration
        "ssl.cipher.suites": str,
        "ssl.curves.list": str,
        "ssl.sigalgs.list": str,
        "ssl.key.location": str,
        "ssl.key.password": str,
        "ssl.key.pem": str,
        "ssl.certificate.location": str,
        "ssl.certificate.pem": str,
        "ssl.ca.location": str,
        "ssl.ca.pem": str,
        "ssl.ca.certificate.stores": str,
        "ssl.crl.location": str,
        "ssl.keystore.location": str,
        "ssl.keystore.password": str,
        "ssl.providers": str,
        "ssl.engine.id": str,
        "enable.ssl.certificate.verification": bool,
        "ssl.endpoint.identification.algorithm": Literal["none", "https"],
        # HTTPS Configuration
        "https.ca.location": str,
        "https.ca.pem": str,
        # SASL Configuration
        "sasl.mechanisms": str,
        "sasl.mechanism": str,
        "sasl.kerberos.service.name": str,
        "sasl.kerberos.principal": str,
        "sasl.kerberos.kinit.cmd": str,
        "sasl.kerberos.keytab": str,
        "sasl.kerberos.min.time.before.relogin": int,
        "sasl.username": str,
        "sasl.password": str,
        # OAUTHBEARER Configuration
        "sasl.oauthbearer.config": str,
        "enable.sasl.oauthbearer.unsecure.jwt": bool,
        "sasl.oauthbearer.method": Literal["default", "oidc"],
        "sasl.oauthbearer.client.id": str,
        "sasl.oauthbearer.client.credentials.client.id": str,
        "sasl.oauthbearer.client.secret": str,
        "sasl.oauthbearer.client.credentials.client.secret": str,
        "sasl.oauthbearer.scope": str,
        "sasl.oauthbearer.extensions": str,
        "sasl.oauthbearer.token.endpoint.url": str,
        "sasl.oauthbearer.grant.type": Literal[
            "client_credentials", "urn:ietf:params:oauth:grant-type:jwt-bearer"
        ],
        "sasl.oauthbearer.assertion.algorithm": Literal["RS256", "ES256"],
        "sasl.oauthbearer.assertion.private.key.file": str,
        "sasl.oauthbearer.assertion.private.key.passphrase": str,
        "sasl.oauthbearer.assertion.private.key.pem": str,
        "sasl.oauthbearer.assertion.file": str,
        "sasl.oauthbearer.assertion.claim.aud": str,
        "sasl.oauthbearer.assertion.claim.exp.seconds": int,
        "sasl.oauthbearer.assertion.claim.iss": str,
        "sasl.oauthbearer.assertion.claim.jti.include": bool,
        "sasl.oauthbearer.assertion.claim.nbf.seconds": int,
        "sasl.oauthbearer.assertion.claim.sub": str,
        "sasl.oauthbearer.assertion.jwt.template.file": str,
        "sasl.oauthbearer.metadata.authentication.type": Literal["none", "azure_imds"],
        # Plugin Configuration
        "plugin.library.paths": str,
        # Client Rack
        "client.rack": str,
        # Metrics
        "enable.metrics.push": bool,
        # DNS
        "client.dns.lookup": Literal[
            "use_all_dns_ips", "resolve_canonical_bootstrap_servers_only"
        ],
        # === Consumer-specific properties ===
        # Group Configuration
        "group.id": str,  # Client group id. Required for consumer groups
        "group.instance.id": str,  # Static group membership. Requires broker >= 2.3.0
        "partition.assignment.strategy": str,  # Assignment strategy. Default: range,roundrobin
        "session.timeout.ms": int,  # Session timeout. Range: 1-3600000. Default: 45000
        "heartbeat.interval.ms": int,  # Heartbeat interval. Range: 1-3600000. Default: 3000
        "group.protocol.type": str,  # Protocol type for classic. Default: consumer
        "group.protocol": Literal[
            "classic", "consumer"
        ],  # Group protocol. Default: classic
        "group.remote.assignor": str,  # Server side assignor: uniform, range
        "coordinator.query.interval.ms": int,  # Coordinator query interval. Range: 1-3600000. Default: 600000
        "max.poll.interval.ms": int,  # Max time between consume calls. Range: 1-86400000. Default: 300000
        # Offset Configuration
        "enable.auto.commit": bool,  # Auto commit offsets. Default: true
        "auto.commit.interval.ms": int,  # Commit frequency. Range: 0-86400000. Default: 5000
        "enable.auto.offset.store": bool,  # Auto store offset. Default: true
        # Queue Configuration
        "queued.min.messages": int,  # Min messages in queue. Range: 1-10000000. Default: 100000
        "queued.max.messages.kbytes": int,  # Max KB in queue. Range: 1-2097151. Default: 65536
        # Fetch Configuration
        "fetch.wait.max.ms": int,  # Max wait for fetch response. Range: 0-300000. Default: 500
        "fetch.queue.backoff.ms": int,  # Backoff when thresholds exceeded. Range: 0-300000. Default: 1000
        "fetch.message.max.bytes": int,  # Max bytes per partition to fetch. Range: 1-1000000000. Default: 1048576
        "max.partition.fetch.bytes": int,  # Alias for fetch.message.max.bytes
        "fetch.max.bytes": int,  # Max data per Fetch request. Range: 0-2147483135. Default: 52428800
        "fetch.min.bytes": int,  # Min bytes for broker response. Range: 1-100000000. Default: 1
        "fetch.error.backoff.ms": int,  # Backoff on fetch error. Range: 0-300000. Default: 500
        # Isolation Level
        "isolation.level": Literal[
            "read_uncommitted", "read_committed"
        ],  # Transaction isolation. Default: read_committed
        # Partition EOF
        "enable.partition.eof": bool,  # Emit partition EOF events. Default: false
        # CRC Check
        "check.crcs": bool,  # Verify CRC32 of messages. Default: false
        # === Consumer Topic Configuration ===
        "auto.offset.reset": Literal[
            "smallest", "earliest", "beginning", "largest", "latest", "end", "error"
        ],  # Offset reset action. Default: largest
        "consume.callback.max.messages": int,  # Max messages per callback. Range: 0-1000000. Default: 0
    },
    total=False,
)

# Producer configuration properties (extends CommonConfig conceptually)
ProducerConfig = TypedDict(
    "ProducerConfig",
    {
        # === All CommonConfig properties ===
        # Basic Configuration
        "builtin.features": str,
        "client.id": str,
        "metadata.broker.list": str,
        "bootstrap.servers": str,
        # Message Size Configuration
        "message.max.bytes": int,
        "message.copy.max.bytes": int,
        "receive.message.max.bytes": int,
        # In-Flight Requests
        "max.in.flight.requests.per.connection": int,
        "max.in.flight": int,
        # Metadata Configuration
        "metadata.recovery.strategy": Literal["none", "rebootstrap"],
        "metadata.recovery.rebootstrap.trigger.ms": int,
        "topic.metadata.refresh.interval.ms": int,
        "metadata.max.age.ms": int,
        "topic.metadata.refresh.fast.interval.ms": int,
        "topic.metadata.refresh.sparse": bool,
        "topic.metadata.propagation.max.ms": int,
        "topic.blacklist": str,
        # Debug
        "debug": str,
        # Socket Configuration
        "socket.timeout.ms": int,
        "socket.send.buffer.bytes": int,
        "socket.receive.buffer.bytes": int,
        "socket.keepalive.enable": bool,
        "socket.nagle.disable": bool,
        "socket.max.fails": int,
        "socket.connection.setup.timeout.ms": int,
        # Broker Configuration
        "broker.address.ttl": int,
        "broker.address.family": Literal["any", "v4", "v6"],
        # Connection Configuration
        "connections.max.idle.ms": int,
        "reconnect.backoff.ms": int,
        "reconnect.backoff.max.ms": int,
        # Retry Configuration
        "retry.backoff.ms": int,
        "retry.backoff.max.ms": int,
        # Statistics
        "statistics.interval.ms": int,
        "enabled_events": int,
        # Logging
        "log_level": int,
        "log.queue": bool,
        "log.thread.name": bool,
        "log.connection.close": bool,
        # Misc
        "enable.random.seed": bool,
        "internal.termination.signal": int,
        "api.version.request.timeout.ms": int,
        "allow.auto.create.topics": bool,
        # Security Protocol
        "security.protocol": Literal["plaintext", "ssl", "sasl_plaintext", "sasl_ssl"],
        # SSL Configuration
        "ssl.cipher.suites": str,
        "ssl.curves.list": str,
        "ssl.sigalgs.list": str,
        "ssl.key.location": str,
        "ssl.key.password": str,
        "ssl.key.pem": str,
        "ssl.certificate.location": str,
        "ssl.certificate.pem": str,
        "ssl.ca.location": str,
        "ssl.ca.pem": str,
        "ssl.ca.certificate.stores": str,
        "ssl.crl.location": str,
        "ssl.keystore.location": str,
        "ssl.keystore.password": str,
        "ssl.providers": str,
        "ssl.engine.id": str,
        "enable.ssl.certificate.verification": bool,
        "ssl.endpoint.identification.algorithm": Literal["none", "https"],
        # HTTPS Configuration
        "https.ca.location": str,
        "https.ca.pem": str,
        # SASL Configuration
        "sasl.mechanisms": str,
        "sasl.mechanism": str,
        "sasl.kerberos.service.name": str,
        "sasl.kerberos.principal": str,
        "sasl.kerberos.kinit.cmd": str,
        "sasl.kerberos.keytab": str,
        "sasl.kerberos.min.time.before.relogin": int,
        "sasl.username": str,
        "sasl.password": str,
        # OAUTHBEARER Configuration
        "sasl.oauthbearer.config": str,
        "enable.sasl.oauthbearer.unsecure.jwt": bool,
        "sasl.oauthbearer.method": Literal["default", "oidc"],
        "sasl.oauthbearer.client.id": str,
        "sasl.oauthbearer.client.credentials.client.id": str,
        "sasl.oauthbearer.client.secret": str,
        "sasl.oauthbearer.client.credentials.client.secret": str,
        "sasl.oauthbearer.scope": str,
        "sasl.oauthbearer.extensions": str,
        "sasl.oauthbearer.token.endpoint.url": str,
        "sasl.oauthbearer.grant.type": Literal[
            "client_credentials", "urn:ietf:params:oauth:grant-type:jwt-bearer"
        ],
        "sasl.oauthbearer.assertion.algorithm": Literal["RS256", "ES256"],
        "sasl.oauthbearer.assertion.private.key.file": str,
        "sasl.oauthbearer.assertion.private.key.passphrase": str,
        "sasl.oauthbearer.assertion.private.key.pem": str,
        "sasl.oauthbearer.assertion.file": str,
        "sasl.oauthbearer.assertion.claim.aud": str,
        "sasl.oauthbearer.assertion.claim.exp.seconds": int,
        "sasl.oauthbearer.assertion.claim.iss": str,
        "sasl.oauthbearer.assertion.claim.jti.include": bool,
        "sasl.oauthbearer.assertion.claim.nbf.seconds": int,
        "sasl.oauthbearer.assertion.claim.sub": str,
        "sasl.oauthbearer.assertion.jwt.template.file": str,
        "sasl.oauthbearer.metadata.authentication.type": Literal["none", "azure_imds"],
        # Plugin Configuration
        "plugin.library.paths": str,
        # Client Rack
        "client.rack": str,
        # Metrics
        "enable.metrics.push": bool,
        # DNS
        "client.dns.lookup": Literal[
            "use_all_dns_ips", "resolve_canonical_bootstrap_servers_only"
        ],
        # === Producer-specific properties ===
        # Transaction Configuration
        "transactional.id": str,  # Transactional producer ID. Requires broker >= 0.11.0
        "transaction.timeout.ms": int,  # Transaction timeout. Range: 1000-2147483647. Default: 60000
        # Idempotence
        "enable.idempotence": bool,  # Exactly-once delivery. Default: false
        "enable.gapless.guarantee": bool,  # EXPERIMENTAL: Fatal error on gaps. Default: false
        # Queue Buffering
        "queue.buffering.max.messages": int,  # Max messages in queue. Range: 0-2147483647. Default: 100000
        "queue.buffering.max.kbytes": int,  # Max KB in queue. Range: 1-2147483647. Default: 1048576
        "queue.buffering.max.ms": float,  # Wait time for batching. Range: 0-900000. Default: 5
        "linger.ms": float,  # Alias for queue.buffering.max.ms
        "queue.buffering.backpressure.threshold": int,  # Backpressure threshold. Range: 1-1000000. Default: 1
        # Retry Configuration
        "message.send.max.retries": int,  # Max retries. Range: 0-2147483647. Default: 2147483647
        "retries": int,  # Alias for message.send.max.retries
        # Compression
        "compression.codec": Literal[
            "none", "gzip", "snappy", "lz4", "zstd"
        ],  # Compression. Default: none
        "compression.type": Literal[
            "none", "gzip", "snappy", "lz4", "zstd"
        ],  # Alias for compression.codec
        # Batching
        "batch.num.messages": int,  # Max messages per batch. Range: 1-1000000. Default: 10000
        "batch.size": int,  # Max batch size in bytes. Range: 1-2147483647. Default: 1000000
        # Delivery Reports
        "delivery.report.only.error": bool,  # Only report failures. Default: false
        # Sticky Partitioning
        "sticky.partitioning.linger.ms": int,  # Sticky partition delay. Range: 0-900000. Default: 10
        # === Producer Topic Configuration ===
        "request.required.acks": int,  # Acks required. Range: -1-1000. Default: -1 (all)
        "acks": int,  # Alias for request.required.acks
        "request.timeout.ms": int,  # Ack timeout. Range: 1-900000. Default: 30000
        "message.timeout.ms": int,  # Local message timeout. Range: 0-2147483647. Default: 300000
        "delivery.timeout.ms": int,  # Alias for message.timeout.ms
        "partitioner": Literal[
            "random",
            "consistent",
            "consistent_random",
            "murmur2",
            "murmur2_random",
            "fnv1a",
            "fnv1a_random",
        ],  # Partitioner. Default: consistent_random
        "compression.level": int,  # Compression level. Range: -1-12. Default: -1
    },
    total=False,
)
