import asyncio
import os
import signal
from ipaddress import IPv4Address, IPv6Address, ip_address
from pathlib import Path

import click
from cloakbrowser import launch_persistent_context_async


def parse_ip_address(
    _ctx: click.Context,
    _param: click.Parameter,
    value: str,
) -> IPv4Address | IPv6Address:
    try:
        return ip_address(value)
    except ValueError as error:
        raise click.BadParameter(str(error)) from error


async def serve_browser(
    address: IPv4Address | IPv6Address,
    cdp_port: int,
    profile_dir: Path,
    headless: bool,
    *,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Run a persistent browser until stopped or unexpectedly disconnected."""
    profile_dir.mkdir(parents=True, exist_ok=True)
    context = await launch_persistent_context_async(
        profile_dir,
        license_key=os.getenv("CLOAKBROWSER_LICENSE_KEY"),
        headless=headless,
        geoip=True,
        args=[
            f"--remote-debugging-address={address}",
            f"--remote-debugging-port={cdp_port}",
        ],
    )

    shutdown = stop_event if stop_event is not None else asyncio.Event()
    browser_disconnected = False
    registered_signals: list[signal.Signals] = []

    def handle_browser_disconnect(_: object) -> None:
        nonlocal browser_disconnected
        browser_disconnected = True
        shutdown.set()

    browser = context.browser
    try:
        if browser is None:
            raise RuntimeError("persistent browser context has no browser")

        browser.on("disconnected", handle_browser_disconnect)
        loop = asyncio.get_running_loop()
        for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(shutdown_signal, shutdown.set)
            registered_signals.append(shutdown_signal)

        if not context.pages:
            await context.new_page()

        click.echo(f"CloakBrowser CDP endpoint: http://{address}:{cdp_port}")
        click.echo(f"Persistent profile: {profile_dir}")
        click.echo(f"Browser mode: {'headless' if headless else 'headed'}")

        await shutdown.wait()
        restart_required = browser_disconnected
    finally:
        if "loop" in locals():
            for shutdown_signal in registered_signals:
                loop.remove_signal_handler(shutdown_signal)
        await context.close()

    if restart_required:
        raise RuntimeError("CloakBrowser disconnected unexpectedly")


@click.command()
@click.option(
    "--cdp-port",
    type=click.IntRange(1024, 65535),
    default=9222,
    show_default=True,
    help="TCP port for the Chrome DevTools Protocol endpoint.",
)
@click.option(
    "--address",
    callback=parse_ip_address,
    default="127.0.0.1",
    show_default=True,
    metavar="IP",
    help="IP address on which to expose the CDP server.",
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
def main(
    cdp_port: int,
    profile_dir: Path,
    headless: bool,
    address: IPv4Address | IPv6Address,
) -> None:
    """Run one persistent CloakBrowser instance with a CDP endpoint."""
    asyncio.run(serve_browser(address, cdp_port, profile_dir, headless))


if __name__ == "__main__":
    main()
