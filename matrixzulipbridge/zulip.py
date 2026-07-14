# MatrixZulipBridge - an appservice puppeting bridge for Matrix - Zulip
#
# Copyright (C) 2024 Emma Meijere <emgh@em.id.lv>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
# Originally licensed under the MIT (Expat) license:
# <https://github.com/hifi/heisenbridge/blob/2532905f13835762870de55ba8a404fad6d62d81/LICENSE>.
#
# [This file includes modifications made by Emma Meijere]
#
#
import asyncio
import logging
import re
import urllib.parse
from typing import TYPE_CHECKING, Optional
from urllib.parse import urljoin

from matrixzulipbridge.media_proxy import sign_resource

import emoji
from aiohttp import BasicAuth
from bs4 import BeautifulSoup
from markdownify import markdownify
from mautrix.types import MediaMessageEventContent, MessageType
from zulip_emoji_mapping import EmojiNotFoundException, ZulipEmojiMapping

from matrixzulipbridge.direct_room import DirectRoom
from matrixzulipbridge.stream_room import StreamRoom
from matrixzulipbridge.types import ZulipUserID

if TYPE_CHECKING:
    from matrixzulipbridge.organization_room import OrganizationRoom
    from matrixzulipbridge.types import ZulipMessageID, ZulipStreamID


class ZulipEventHandler:
    def __init__(self, organization: "OrganizationRoom") -> None:
        self.organization = organization
        self.messages = set()
        self.loop = asyncio.get_running_loop()

    def on_event(self, event: dict):
        logging.debug(f"Zulip event for {self.organization.name}: {event}")
        try:
            match event["type"]:
                case "message":
                    self._handle_message(event["message"])
                case "subscription":
                    self._handle_subscription(event)
                case "reaction":
                    self._handle_reaction(event)
                case "delete_message":
                    self._handle_delete_message(event)
                case "realm_user":
                    self._handle_realm_user(event)
                case "update_message":
                    self._handle_update_message(event)
                case _:
                    logging.debug(f"Unhandled event type: {event['type']}")
        except Exception as e:  # pylint: disable=broad-exception-caught
            logging.exception(e)

    def backfill_message(self, message: dict):
        self._handle_message(message)

    def _handle_message(self, event: dict):
        if event["type"] != "stream":
            return
        if event["sender_id"] == self.organization.profile["user_id"]:
            return  # Ignore own messages
        # Prevent race condition when single message is received by multiple clients
        if str(event["id"]) in self.messages:
            return
        self.messages.add(str(event["id"]))
        asyncio.run_coroutine_threadsafe(self._forward_stream_message(event), self.loop)

    async def _forward_stream_message(self, event: dict):
        room = self._get_room_by_stream_id(event["stream_id"])
        if not room:
            logging.debug(
                f"Received message from stream with no associated Matrix room: {event}"
            )
            return

        # Skip already forwarded messages
        if str(event["id"]) in room.messages:
            return

        topic = event["subject"]
        mx_user_id = room.serv.get_mxid_from_zulip_user_id(
            self.organization, event["sender_id"]
        )

        message, formatted_message, reply_event_id, inline_images = self._process_message_content(
            event["content"], room
        )

        last_image_event_id = None
        for img in inline_images:
            last_image_event_id = await self._send_zulip_image_to_matrix(room, img, mx_user_id)

        custom_data = {
            "zulip_topic": topic,
            "zulip_user_id": event["sender_id"],
            "display_name": event["sender_full_name"],
            "zulip_message_id": event["id"],
            "type": "message",
            "timestamp": event["timestamp"],
            "target": "stream",
            "reply_to": reply_event_id,
        }

        if message.strip():
            room.send_message(
                message,
                formatted=formatted_message,
                user_id=mx_user_id,
                custom_data=custom_data,
            )
        elif last_image_event_id is not None:
            # Image-only message: track the Zulip message ID so it isn't resent on restart
            room.messages[str(event["id"])] = last_image_event_id
            await room.save()

    async def _send_zulip_image_to_matrix(self, room, img: dict, mx_user_id: str) -> Optional[str]:
        org = self.organization
        if not org.email or not org.api_key:
            return None
        zulip_url = f"{org.site}/user_uploads/{img['path']}"
        auth = BasicAuth(org.email, org.api_key)
        try:
            async with org.serv.az.http_session.get(zulip_url, auth=auth) as resp:
                if resp.status != 200:
                    logging.warning("Failed to download Zulip image %s: %d", zulip_url, resp.status)
                    return None
                data = await resp.read()
                mime_type = resp.headers.get("Content-Type", "application/octet-stream").split(";")[0].strip()
        except Exception:
            logging.warning("Failed to download Zulip image %s", zulip_url, exc_info=True)
            return None
        try:
            intent = org.serv.az.intent.user(mx_user_id)
            mxc = await intent.upload_media(data, mime_type=mime_type, filename=img["filename"])
            return await intent.send_message(
                room.id,
                MediaMessageEventContent(
                    msgtype=MessageType.IMAGE,
                    body=img["filename"],
                    url=mxc,
                ),
            )
        except Exception:
            logging.warning("Failed to send Zulip image to Matrix", exc_info=True)
            return None

    async def handle_dm_message(self, event: dict):
        if event["sender_id"] == self.organization.profile["user_id"]:
            return  # Ignore own messages
        # Prevent race condition when single message is received by multiple clients
        if str(event["id"]) in self.messages:
            return
        mx_user_id = self.organization.serv.get_mxid_from_zulip_user_id(
            self.organization, event["sender_id"]
        )
        recipient_ids = frozenset(user["id"] for user in event["display_recipient"])
        room = self.organization.direct_rooms.get(recipient_ids)
        if not room:
            room = await DirectRoom.create(
                self.organization, event["display_recipient"]
            )

        # Skip already forwarded messages
        if str(event["id"]) in room.messages:
            return

        message, formatted_message, reply_event_id, inline_images = self._process_message_content(
            event["content"], room
        )

        last_image_event_id = None
        for img in inline_images:
            last_image_event_id = await self._send_zulip_image_to_matrix(room, img, mx_user_id)

        custom_data = {
            "zulip_user_id": event["sender_id"],
            "display_name": event["sender_full_name"],
            "zulip_message_id": event["id"],
            "type": "message",
            "timestamp": event["timestamp"],
            "target": "direct",
            "reply_to": reply_event_id,
        }

        if message.strip():
            room.send_message(
                message,
                formatted=formatted_message,
                user_id=mx_user_id,
                custom_data=custom_data,
            )
        elif last_image_event_id is not None:
            # Image-only message: track the Zulip message ID so it isn't resent on restart
            room.messages[str(event["id"])] = last_image_event_id
            await room.save()

    def _handle_reaction(self, event: dict):
        zulip_message_id = str(event["message_id"])
        room = self._get_room_by_message_id(zulip_message_id)

        if not room:
            logging.debug(f"Couldn't find room for reaction: {event}")
            return

        mx_user_id = room.serv.get_mxid_from_zulip_user_id(
            self.organization, event["user_id"]
        )

        try:
            reaction = ZulipEmojiMapping.get_emoji_by_name(event["emoji_name"])
        except EmojiNotFoundException:
            reaction = event["emoji_name"]

        if event["op"] == "add":
            message_event_id = room.messages[zulip_message_id]
            room.relay_zulip_react(
                user_id=mx_user_id,
                event_id=message_event_id,
                key=reaction,
                zulip_message_id=zulip_message_id,
                zulip_emoji_name=event["emoji_name"],
                zulip_user_id=ZulipUserID(event["user_id"]),
            )
        elif event["op"] == "remove":
            request = {
                "message_id": zulip_message_id,
                "emoji_name": event["emoji_name"],
                "user_id": ZulipUserID(event["user_id"]),
            }
            frozen_request = frozenset(request.items())

            event_id = room.reactions.inverse.get(frozen_request)

            if event_id is None:
                return

            room.redact(event_id, "removed on Zulip")
            del room.reactions[event_id]

    def _handle_delete_message(self, event: dict):
        message_id = str(event["message_id"])

        if event.get("message_type") == "stream":
            room = self._get_room_by_stream_id(event["stream_id"])
        else:
            room = self._get_room_by_message_id(message_id)

        if not room:
            return

        message_mxid = room.messages.get(message_id)
        if not message_mxid:
            return

        room.redact(message_mxid, reason="Deleted on Zulip")
        del room.messages[message_id]

    def _handle_subscription(self, event: dict):
        if not "stream_ids" in event:
            return
        for stream_id in event["stream_ids"]:
            room = self._get_room_by_stream_id(stream_id)

            if not room:
                logging.debug(
                    f"Received message from stream with no associated Matrix room: {event}"
                )
                return

            match event["op"]:
                case "peer_add":
                    for user_id in event["user_ids"]:
                        room.on_join(user_id)
                case "peer_remove":
                    for user_id in event["user_ids"]:
                        room.on_part(user_id)

    def _handle_realm_user(self, event: dict):
        # Update Zulip user cache
        if event["op"] == "update":
            user_id = event["person"]["user_id"]
            if not user_id in self.organization.zulip_users:
                return
            self.organization.zulip_users[user_id] |= event["person"]

    def _handle_update_message(self, event: dict):
        if "orig_subject" in event:
            # Message topic renamed
            stream_id = event.get("stream_id")
            if stream_id is None:
                return
            room = self._get_room_by_stream_id(stream_id)
            if event["propagate_mode"] == "change_all":
                thread_event_id = room.threads.get(event["orig_subject"])
                if thread_event_id is None:
                    return
                del room.threads[event["orig_subject"]]
                room.threads[event["subject"]] = thread_event_id

    def _get_mxid_from_zulip_id(
        self, zulip_id: "ZulipMessageID", room: DirectRoom = None
    ):
        if room is not None:
            return room.messages.get(str(zulip_id))

        for room in self.organization.rooms.values():
            if not isinstance(room, DirectRoom):
                continue
            mxid = room.messages.get(str(zulip_id))
            if mxid is not None:
                return mxid

        logging.debug(
            f"Message with Zulip ID {zulip_id} not found, it probably wasn't sent to Matrix"
        )

    def _get_room_by_stream_id(
        self, stream_id: "ZulipStreamID"
    ) -> Optional["StreamRoom"]:
        for room in self.organization.rooms.values():
            if not isinstance(room, StreamRoom):
                continue
            if room.stream_id == stream_id:
                return room
        return None

    def _get_room_by_message_id(
        self, message_id: "ZulipMessageID"
    ) -> Optional["DirectRoom"]:
        for room in self.organization.rooms.values():
            if not isinstance(room, DirectRoom):
                continue
            if message_id in room.messages:
                return room
        return None

    def _process_message_content(self, html: str, room: "DirectRoom"):
        reply_event_id = None
        inline_images = []

        soup = BeautifulSoup(html, "html.parser")

        proxy_url = self.organization.serv.proxy_url
        zulip_host = (
            urllib.parse.urlparse(self.organization.site).netloc
            if self.organization.site
            else None
        )

        secret = self.organization.serv.registration["as_token"]

        def _rewrite_url(url: str) -> str:
            if url and url.startswith("/user_uploads/") and proxy_url and zulip_host:
                path = url[len("/user_uploads/"):]
                resource = f"zulip/{zulip_host}/{path}"
                sig = sign_resource(secret, resource)
                return f"{proxy_url}/media/{resource}?sig={sig}"
            return urljoin(self.organization.server["realm_uri"], url)

        # Collect inline images to upload to Matrix as m.image events, then remove
        # the div from the message body (images are sent as separate events).
        inline_image_hrefs = set()
        for div in soup.find_all("div", class_="message_inline_image"):
            a_tag = div.find("a")
            if a_tag:
                href = a_tag.get("href", "")
                if href.startswith("/user_uploads/"):
                    path = href[len("/user_uploads/"):]
                    filename = urllib.parse.unquote(path.split("/")[-1]) or "image"
                    inline_images.append({"path": path, "filename": filename})
                    inline_image_hrefs.add(href)
            div.decompose()

        # Remove links that correspond to inline images (Zulip renders both a link
        # in the paragraph text and the message_inline_image div).
        for a_tag in soup.find_all("a"):
            if a_tag.get("href") in inline_image_hrefs:
                a_tag.decompose()

        # Clean up paragraphs that became empty after removing image links.
        for p in soup.find_all("p"):
            while p.contents and getattr(p.contents[-1], "name", None) == "br":
                p.contents[-1].decompose()
            if not p.get_text(strip=True):
                p.decompose()

        for a_tag in soup.find_all("a"):
            href = a_tag.get("href")
            if href:
                a_tag["href"] = _rewrite_url(href)

        for img_tag in soup.find_all("img"):
            src = img_tag.get("src")
            if src:
                img_tag["src"] = _rewrite_url(src)

        # Check if message contains a reply
        first_text = soup.find("p")
        if first_text is not None:
            mentioned_user = first_text.select("span.user-mention.silent")
            narrow_link = first_text.find("a")
        quote = soup.find("blockquote")
        if (
            first_text is not None
            and len(mentioned_user) == 1
            and narrow_link is not None
            and narrow_link.get("href") is not None
            and quote is not None
            and "#narrow" in narrow_link.get("href", "")
        ):
            # Parse reply (crudely?)
            message_id = re.match(r".*\/near\/(\d+)(\/|$)", narrow_link.get("href"))[1]
            reply_event_id = room.messages.get(message_id)

            # Create rich reply fallback
            if reply_event_id is not None:
                mentioned_zulip_id = mentioned_user[0]["data-user-id"]
                mentioned_user_mxid = self.organization.zulip_puppet_user_mxid.get(
                    mentioned_zulip_id
                )
                if mentioned_user_mxid is None:
                    mentioned_user_mxid = (
                        self.organization.serv.get_mxid_from_zulip_user_id(
                            self.organization, mentioned_zulip_id
                        )
                    )

                quote.extract()

                # Fromat reply
                mx_reply = soup.new_tag("mx-reply")
                mx_reply_quote = soup.new_tag("blockquote")

                mx_reply_event = soup.new_tag(
                    "a",
                    href=f"https://matrix.to/#/{room.id}/{reply_event_id}",
                )
                mx_reply_event.append(soup.new_string("In reply to"))

                mx_reply_author = soup.new_tag(
                    "a", href=f"https://matrix.to/#/{mentioned_user_mxid}"
                )
                mx_reply_author.append(soup.new_string(mentioned_user_mxid))

                mx_reply_quote.append(mx_reply_event)
                mx_reply_quote.append(mx_reply_author)
                mx_reply_quote.append(soup.new_tag("br"))

                for child in quote.findChildren():
                    mx_reply_quote.append(child)

                mx_reply.append(mx_reply_quote)

                first_text.replace_with(mx_reply)

        formatted_message = emoji.emojize(soup.decode(), language="alias")
        message = markdownify(formatted_message).rstrip()
        return message, formatted_message, reply_event_id, inline_images
