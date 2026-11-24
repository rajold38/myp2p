# User-card Login button

1. Deploy the updated bot files together, including admin-login.js.
2. Replace your website HTML with public/index.html from this archive.
3. Set ADMIN_LOGIN_APP_URL to the exact HTTPS address of that updated website; the existing biexc.in address is the default.
4. In your administrator Telegram chat, open a user card, tap Login, then tap Open account.

Links expire after five minutes, work once, and are invalidated by bot restarts or generating a replacement for the same user. Keep one bot instance as the existing deployment expects. In private chats only the configured administrator can generate links; in groups only Telegram owners/administrators can do so. Anyone possessing a generated link can use it, so do not forward links. Access uses the existing Firebase account, without changing profile records. Audit entries are recorded under botMeta/adminLoginAudit without link secrets.

The HTML still uses its existing backend address. If the bot URL changes, update window.__BACKEND_URL__ in the HTML. Protect botMeta with deny-all client database rules; the Firebase Admin SDK still has access.

Tests: node --test admin-login.test.js

Live Telegram/Firebase account access has not been tested here because deployment credentials were not supplied. Local automated checks use stubbed authentication and do not modify real users. Both website and bot changes must be deployed before the feature works.
