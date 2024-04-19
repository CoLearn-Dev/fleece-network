import asyncio
import binascii
import datetime
import enum
import logging
import os
import traceback
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Optional, Type, TypeVar

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from OpenSSL import SSL, crypto
from pyee.asyncio import AsyncIOEventEmitter

if TYPE_CHECKING:
    from .rtcsctptransport import RTCSctpTransport

from . import clock, rtp
from .rtcicetransport import RTCIceTransport
from .stats import RTCStatsReport, RTCTransportStats

CERTIFICATE_T = TypeVar("CERTIFICATE_T", bound="RTCCertificate")

logger = logging.getLogger(__name__)


def certificate_digest(x509: crypto.X509) -> str:
    return x509.digest("SHA256").decode("ascii")


def generate_certificate(key: ec.EllipticCurvePrivateKey) -> x509.Certificate:
    name = x509.Name(
        [
            x509.NameAttribute(
                x509.NameOID.COMMON_NAME,
                binascii.hexlify(os.urandom(16)).decode("ascii"),
            )
        ]
    )
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    builder = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=30))
    )
    return builder.sign(key, hashes.SHA256(), default_backend())


class State(enum.Enum):
    NEW = 0
    CONNECTING = 1
    CONNECTED = 2
    CLOSED = 3
    FAILED = 4


@dataclass
class RTCDtlsFingerprint:
    """
    The :class:`RTCDtlsFingerprint` dictionary includes the hash function
    algorithm and certificate fingerprint.
    """

    algorithm: str
    "The hash function name, for instance `'sha-256'`."

    value: str
    "The fingerprint value."


class RTCCertificate:
    """
    The :class:`RTCCertificate` interface enables the certificates used by an
    :class:`RTCDtlsTransport`.

    To generate a certificate and the corresponding private key use
    :func:`generateCertificate`.
    """

    def __init__(self, key: crypto.PKey, cert: crypto.X509) -> None:
        self._key = key
        self._cert = cert

    @property
    def expires(self) -> datetime.datetime:
        """
        The date and time after which the certificate will be considered invalid.
        """
        return self._cert.to_cryptography().not_valid_after_utc

    def getFingerprints(self) -> List[RTCDtlsFingerprint]:
        """
        Returns the list of certificate fingerprints, one of which is computed
        with the digest algorithm used in the certificate signature.
        """
        return [
            RTCDtlsFingerprint(
                algorithm="sha-256",
                value=certificate_digest(self._cert),
            )
        ]

    @classmethod
    def generateCertificate(cls: Type[CERTIFICATE_T]) -> CERTIFICATE_T:
        """
        Create and return an X.509 certificate and corresponding private key.

        :rtype: RTCCertificate
        """
        key = ec.generate_private_key(ec.SECP256R1(), default_backend())
        cert = generate_certificate(key)
        return cls(
            key=crypto.PKey.from_cryptography_key(key),  # type: ignore
            cert=crypto.X509.from_cryptography(cert),
        )

    def _create_ssl_context(self) -> SSL.Context:
        ctx = SSL.Context(SSL.DTLS_METHOD)
        ctx.set_verify(
            SSL.VERIFY_PEER | SSL.VERIFY_FAIL_IF_NO_PEER_CERT, lambda *args: True
        )
        ctx.use_certificate(self._cert)
        ctx.use_privatekey(self._key)
        ctx.set_cipher_list(
            b"ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-ECDSA-AES128-SHA:ECDHE-ECDSA-AES256-SHA"
        )

        return ctx


@dataclass
class RTCDtlsParameters:
    """
    The :class:`RTCDtlsParameters` dictionary includes information relating to
    DTLS configuration.
    """

    fingerprints: List[RTCDtlsFingerprint] = field(default_factory=list)
    "List of :class:`RTCDtlsFingerprint`, one fingerprint for each certificate."

    role: Optional[str] = "auto"
    "The DTLS role, with a default of auto."


class RTCDtlsTransport(AsyncIOEventEmitter):
    """
    The :class:`RTCDtlsTransport` object includes information relating to
    Datagram Transport Layer Security (DTLS) transport.

    :param transport: An :class:`RTCIceTransport`.
    :param certificates: A list of :class:`RTCCertificate` (only one is allowed
        currently).
    """

    def __init__(
        self, transport: RTCIceTransport, certificates: List[RTCCertificate]
    ) -> None:
        assert len(certificates) == 1
        certificate = certificates[0]

        super().__init__()
        self.encrypted = False
        self._data_receiver: Optional[RTCSctpTransport] = None
        self._role = "auto"
        self._rtp_header_extensions_map = rtp.HeaderExtensionsMap()
        self._state = State.NEW
        self._stats_id = "transport_" + str(id(self))
        self._task: Optional[asyncio.Future[None]] = None
        self._transport = transport

        # counters
        self.__rx_bytes = 0
        self.__rx_packets = 0
        self.__tx_bytes = 0
        self.__tx_packets = 0

        # SSL
        self._ssl: Optional[SSL.Connection] = None
        self.__local_certificate = certificate

    @property
    def state(self) -> str:
        """
        The current state of the DTLS transport.

        One of `'new'`, `'connecting'`, `'connected'`, `'closed'` or `'failed'`.
        """
        return str(self._state)[6:].lower()

    @property
    def transport(self):
        """
        The associated :class:`RTCIceTransport` instance.
        """
        return self._transport

    def getLocalParameters(self) -> RTCDtlsParameters:
        """
        Get the local parameters of the DTLS transport.

        :rtype: :class:`RTCDtlsParameters`
        """
        return RTCDtlsParameters(
            fingerprints=self.__local_certificate.getFingerprints()
        )

    async def start(self, remoteParameters: RTCDtlsParameters) -> None:
        """
        Start DTLS transport negotiation with the parameters of the remote
        DTLS transport.

        :param remoteParameters: An :class:`RTCDtlsParameters`.
        """
        assert self._state == State.NEW
        assert len(remoteParameters.fingerprints)

        # For WebRTC, the DTLS role is explicitly determined as part of the
        # offer / answer exchange.
        #
        # For ORTC however, we determine the DTLS role based on the ICE role.
        if self._role == "auto":
            if self.transport.role == "controlling":
                self._set_role("server")
            else:
                self._set_role("client")

        # Initialise SSL.
        self._ssl = SSL.Connection(self.__local_certificate._create_ssl_context())
        if self._role == "server":
            self._ssl.set_accept_state()
        else:
            self._ssl.set_connect_state()

        self._set_state(State.CONNECTING)
        try:
            while not self.encrypted:
                try:
                    self._ssl.do_handshake()
                except SSL.WantReadError:
                    await self._write_ssl()
                    await self._recv_next()
                except SSL.Error as exc:
                    self.__log_debug("x DTLS handshake failed (error %s)", exc)
                    self._set_state(State.FAILED)
                    return
                else:
                    self.encrypted = True
        except ConnectionError:
            self.__log_debug("x DTLS handshake failed (connection error)")
            self._set_state(State.FAILED)
            return

        # check remote fingerprint
        x509 = self._ssl.get_peer_certificate()
        assert x509
        remote_fingerprint = certificate_digest(x509)
        fingerprint_is_valid = False
        for f in remoteParameters.fingerprints:
            if (
                f.algorithm.lower() == "sha-256"
                and f.value.lower() == remote_fingerprint.lower()
            ):
                fingerprint_is_valid = True
                break
        if not fingerprint_is_valid:
            self.__log_debug("x DTLS handshake failed (fingerprint mismatch)")
            self._set_state(State.FAILED)
            return

        # start data pump
        self.__log_debug("- DTLS handshake complete")
        self._set_state(State.CONNECTED)
        self._task = asyncio.ensure_future(self.__run())

    async def stop(self) -> None:
        """
        Stop and close the DTLS transport.
        """
        if self._task is not None:
            self._task.cancel()
            self._task = None

        if self._ssl and self._state in [State.CONNECTING, State.CONNECTED]:
            try:
                self._ssl.shutdown()
            except SSL.Error:
                pass
            try:
                await self._write_ssl()
            except ConnectionError:
                pass
            self.__log_debug("- DTLS shutdown complete")

    async def __run(self) -> None:
        try:
            while True:
                await self._recv_next()
        except ConnectionError:
            # the connect error does not affect SCTP transport
            pass
        except Exception as exc:
            if not isinstance(exc, asyncio.CancelledError):
                self.__log_warning(traceback.format_exc())
            raise exc
        finally:
            self._set_state(State.CLOSED)

    def _get_stats(self) -> RTCStatsReport:
        report = RTCStatsReport()
        report.add(
            RTCTransportStats(
                # RTCStats
                timestamp=clock.current_datetime(),
                type="transport",
                id=self._stats_id,
                # RTCTransportStats,
                packetsSent=self.__tx_packets,
                packetsReceived=self.__rx_packets,
                bytesSent=self.__tx_bytes,
                bytesReceived=self.__rx_bytes,
                iceRole=self.transport.role,
                dtlsState=self.state,
            )
        )
        return report

    async def _recv_next(self) -> None:
        assert self._ssl

        # get timeout
        timeout = None
        if not self.encrypted:
            timeout = self._ssl.DTLSv1_get_timeout()

        # receive next datagram
        if timeout is not None:
            try:
                data = await asyncio.wait_for(self.transport._recv(), timeout=timeout)
            except asyncio.TimeoutError:
                self.__log_debug("x DTLS handling timeout")
                self._ssl.DTLSv1_handle_timeout()
                await self._write_ssl()
                return
        else:
            data = await self.transport._recv()

        self.__rx_bytes += len(data)
        self.__rx_packets += 1

        first_byte = data[0]
        if first_byte > 19 and first_byte < 64:
            # DTLS
            self._ssl.bio_write(data)
            try:
                data = self._ssl.recv(1500)
            except SSL.ZeroReturnError:
                data = None
            except SSL.Error:
                data = b""
            await self._write_ssl()
            if data is None:
                self.__log_debug("- DTLS shutdown by remote party")
                raise ConnectionError
            elif data and self._data_receiver:
                await self._data_receiver._handle_data(data)
        elif first_byte > 127 and first_byte < 192:
            raise ValueError("SRTP/SRTCP is not supported")

    def _register_data_receiver(self, receiver) -> None:
        assert self._data_receiver is None
        self._data_receiver = receiver

    async def _send_data(self, data: bytes) -> None:
        if self._state != State.CONNECTED:
            raise ConnectionError("Cannot send encrypted data, not connected")

        assert self._ssl
        self._ssl.send(data)
        await self._write_ssl()

    def _set_role(self, role: str) -> None:
        self._role = role

    def _set_state(self, state: State) -> None:
        if state != self._state:
            self.__log_debug("- %s -> %s", self._state, state)
            self._state = state
            self.emit("statechange")

    def _unregister_data_receiver(self, receiver) -> None:
        if self._data_receiver == receiver:
            self._data_receiver = None

    async def _write_ssl(self) -> None:
        """
        Flush outgoing data which OpenSSL put in our BIO to the transport.
        """
        try:
            assert self._ssl
            data = self._ssl.bio_read(1500)
        except SSL.Error:
            data = b""
        if data:
            await self.transport._send(data)
            self.__tx_bytes += len(data)
            self.__tx_packets += 1

    def __log_debug(self, msg: str, *args) -> None:
        logger.debug(f"RTCDtlsTransport(%s) {msg}", self._role, *args)

    def __log_warning(self, msg: str, *args) -> None:
        logger.warning(f"RTCDtlsTransport(%s) {msg}", self._role, *args)
