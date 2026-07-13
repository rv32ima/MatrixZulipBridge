import hashlib
import hmac
import logging
import urllib.parse
from typing import TYPE_CHECKING

from aiohttp import BasicAuth, web

if TYPE_CHECKING:
    from matrixzulipbridge.__main__ import BridgeAppService
    from matrixzulipbridge.organization_room import OrganizationRoom

SIG_LEN = 32  # hex chars = 16 bytes of HMAC-SHA256


def sign_resource(secret: str, resource: str) -> str:
    """Return a truncated HMAC-SHA256 hex signature for the given resource string."""
    return hmac.new(
        secret.encode(), resource.encode(), hashlib.sha256
    ).hexdigest()[:SIG_LEN]


def verify_resource(secret: str, resource: str, sig: str) -> bool:
    """Constant-time check that sig matches the expected signature for resource."""
    if len(sig) != SIG_LEN:
        return False
    return hmac.compare_digest(sign_resource(secret, resource), sig)


class MediaProxy:
    def __init__(self, serv: "BridgeAppService") -> None:
        self.serv = serv

    def register_routes(self, app: web.Application) -> None:
        app.router.add_get(
            "/media/matrix/{server}/{media_id}", self.handle_matrix_media
        )
        app.router.add_get(
            "/media/zulip/{zulip_host}/{path:.*}", self.handle_zulip_media
        )

    async def _stream_proxy(
        self, request: web.Request, url: str, **fetch_kwargs
    ) -> web.StreamResponse:
        async with self.serv.az.http_session.get(url, **fetch_kwargs) as upstream:
            resp = web.StreamResponse(status=upstream.status)
            content_type = upstream.headers.get(
                "Content-Type", "application/octet-stream"
            )
            resp.content_type = content_type.split(";")[0].strip()
            resp.headers["Access-Control-Allow-Origin"] = "*"
            if "Content-Disposition" in upstream.headers:
                resp.headers["Content-Disposition"] = upstream.headers[
                    "Content-Disposition"
                ]
            if "Content-Length" in upstream.headers:
                resp.content_length = int(upstream.headers["Content-Length"])
            await resp.prepare(request)
            async for chunk in upstream.content.iter_chunked(65536):
                await resp.write(chunk)
            await resp.write_eof()
            return resp

    def _check_sig(self, request: web.Request, resource: str) -> bool:
        sig = request.query.get("sig", "")
        return verify_resource(self.serv.registration["as_token"], resource, sig)

    async def handle_matrix_media(self, request: web.Request) -> web.Response:
        server = request.match_info["server"]
        media_id = request.match_info["media_id"]
        if not self._check_sig(request, f"matrix/{server}/{media_id}"):
            return web.Response(status=403, text="Invalid or missing signature")
        url = f"{self.serv.api.base_url}/_matrix/client/v1/media/download/{server}/{media_id}"
        headers = {"Authorization": f"Bearer {self.serv.registration['as_token']}"}
        try:
            return await self._stream_proxy(request, url, headers=headers)
        except Exception:
            logging.exception("Failed to proxy Matrix media %s/%s", server, media_id)
            return web.Response(status=502, text="Failed to fetch media from homeserver")

    async def handle_zulip_media(self, request: web.Request) -> web.Response:
        zulip_host = request.match_info["zulip_host"]
        path = request.match_info["path"]
        if not self._check_sig(request, f"zulip/{zulip_host}/{path}"):
            return web.Response(status=403, text="Invalid or missing signature")

        org = self._find_org_by_host(zulip_host)
        if org is None:
            return web.Response(status=404, text="Organization not found")
        if not org.email or not org.api_key:
            return web.Response(status=503, text="Organization not connected")

        if path.startswith("thumbnail/"):
            zulip_url = f"{org.site}/{path}"
        else:
            zulip_url = f"{org.site}/user_uploads/{path}"
        auth = BasicAuth(org.email, org.api_key)
        try:
            return await self._stream_proxy(request, zulip_url, auth=auth)
        except Exception:
            logging.exception(
                "Failed to proxy Zulip media %s/%s", zulip_host, path
            )
            return web.Response(status=502, text="Failed to fetch media from Zulip")

    def _find_org_by_host(self, host: str) -> "OrganizationRoom | None":
        from matrixzulipbridge.organization_room import OrganizationRoom

        for room in self.serv.find_rooms(OrganizationRoom):
            if room.site and urllib.parse.urlparse(room.site).netloc == host:
                return room
        return None
