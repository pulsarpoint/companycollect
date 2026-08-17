import asyncio
import signal
from pathlib import Path

import click
from cloakbrowser import launch_persistent_context_async


async def serve_browser(
    cdp_port: int,
    profile_dir: Path,
    headless: bool,
) -> None:
    profile_dir.mkdir(parents=True, exist_ok=True)
    context = await launch_persistent_context_async(
        profile_dir,
        headless=headless,
        args=[
            "--remote-debugging-address=127.0.0.1",
            f"--remote-debugging-port={cdp_port}",
        ],
    )

    stop_event = asyncio.Event()
    browser_disconnected = False

    def handle_browser_disconnect(_: object) -> None:
        nonlocal browser_disconnected
        browser_disconnected = True
        stop_event.set()

    browser = context.browser
    try:
        if browser is None:
            raise RuntimeError("persistent browser context has no browser")

        browser.on("disconnected", handle_browser_disconnect)
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, stop_event.set)
        loop.add_signal_handler(signal.SIGTERM, stop_event.set)

        if not context.pages:
            await context.new_page()

        click.echo(f"CloakBrowser CDP endpoint: http://127.0.0.1:{cdp_port}")
        click.echo(f"Persistent profile: {profile_dir}")

        await stop_event.wait()
        restart_required = browser_disconnected
    finally:
        await context.close()

    if restart_required:
        raise RuntimeError("CloakBrowser disconnected unexpectedly")


@click.command()
@click.option(
    "--cdp-port",
    type=click.IntRange(1024, 65535),
    default=9222,
    show_default=True,
    help="Localhost TCP port for the Chrome DevTools Protocol endpoint.",
)
@click.option(
    "--profile-dir",
    type=click.Path(path_type=Path, file_okay=False),
    required=True,
    help="Persistent Chromium profile directory.",
)
@click.option(
    "--headless/--headed",
    default=False,
    show_default=True,
    help="Run without or with a graphical browser window.",
)
def main(cdp_port: int, profile_dir: Path, headless: bool) -> None:
    """Run one persistent CloakBrowser instance with a local CDP endpoint."""
    asyncio.run(serve_browser(cdp_port, profile_dir, headless))


if __name__ == "__main__":
    main()
