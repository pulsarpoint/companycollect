"""Local test website for verifying noVNC input and resuming in the same session."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Website(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/robots.txt":
            status, body = 200, "User-agent: *\nAllow: /"
        elif self.path == "/allow":
            self.send_response(303)
            self.send_header(
                "Set-Cookie", "validation_allowed=yes; Path=/; HttpOnly; SameSite=Lax"
            )
            self.send_header("Location", "/company")
            self.end_headers()
            return
        elif "validation_allowed=yes" not in self.headers.get("Cookie", ""):
            status, body = (
                403,
                """<html><title>Remote browser validation</title>
            <body style="font:24px sans-serif;padding:60px">
            <h1>Remote browser validation</h1><p>This is a controlled test page.</p>
            <p>Click the button, then use Resume crawl in Backoffice.</p>
            <a href="/allow"><button style="font:24px sans-serif;padding:20px">Load test company</button></a>
            </body></html>""",
            )
        elif self.path == "/jobs":
            status, body = (
                200,
                """<html><title>Validation company jobs</title><body>
            <h1>Jobs</h1><h2>Embedded Software Engineer</h2>
            <p>Join the team building embedded systems.</p><a href="mailto:jobs@example.test">Apply</a>
            </body></html>""",
            )
        else:
            status, body = (
                200,
                """<html><title>Validation company</title><body>
            <h1>Validation company</h1><p>We develop embedded software and electronic systems.</p>
            <a href="/jobs">Jobs</a><a href="mailto:team@example.test">Contact us</a>
            </body></html>""",
            )
        self.send_response(status)
        self.send_header(
            "Content-Type", "text/plain" if self.path == "/robots.txt" else "text/html"
        )
        self.send_header("Content-Length", str(len(body.encode())))
        self.end_headers()
        self.wfile.write(body.encode())

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("127.0.0.1", 18182), Website).serve_forever()
