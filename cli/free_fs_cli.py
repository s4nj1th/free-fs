#!/usr/bin/env python3
"""
╔═══════════════════════════════════════════════════════╗
║          FREE-FS - Google File System Replica CLI         ║
║          Distributed File System at your fingertips   ║
╚═══════════════════════════════════════════════════════╝
"""
import grpc
import sys
import os
import time
import math
import threading
import argparse
import hashlib
import json
from pathlib import Path
from datetime import datetime


from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TransferSpeedColumn, TimeRemainingColumn
from rich.live import Live
from rich.layout import Layout
from rich.text import Text
from rich.align import Align
from rich.rule import Rule
from rich import box
from rich.columns import Columns
from rich.prompt import Prompt, Confirm
from rich.syntax import Syntax
from rich.tree import Tree
import rich.spinner


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "proto"))
import free_fs_pb2 as pb
import free_fs_pb2_grpc as pb_grpc
console = Console()
CHUNK_SIZE = 64 * 1024 * 1024  

MASTER_ADDR = os.environ.get("FREE_FS_MASTER", os.environ.get("GFS_MASTER", "localhost:50051"))


COLORS = {
    "primary":   "#00D4FF",
    "secondary": "#FF6B35",
    "success":   "#00FF87",
    "error":     "#FF4757",
    "warning":   "#FFA502",
    "dim":       "#636E72",
    "white":     "#FFFFFF",
    "purple":    "#A55EEA",
    "cyan":      "#00CEC9",
}


BANNER = """
 ██████╗ ███████╗███████╗
██╔════╝ ██╔════╝██╔════╝
██║  ███╗█████╗  ███████╗
██║   ██║██╔══╝  ╚════██║
╚██████╔╝██║     ███████║
 ╚═════╝ ╚═╝     ╚══════╝  [dim]Google File System Replica[/dim]
"""
def show_banner():
    console.print()
    lines = BANNER.strip().split('\n')
    colors = ["#FF6B35", "#FF8C42", "#FFA552", "#FFB865", "#FFCC7A", "#00D4FF"]
    for i, line in enumerate(lines):
        color = colors[i % len(colors)]
        console.print(f"[bold {color}]{line}[/bold {color}]", justify="center")
    console.print()
    console.print(f"[dim]  Master: [bold cyan]{MASTER_ADDR}[/bold cyan]  |  Chunk Size: [bold]64MB[/bold]  |  Replication: [bold]3x[/bold][/dim]", justify="center")
    console.print()
def show_connecting_animation(addr: str):
    frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
    with console.status(f"[bold cyan]Connecting to master @ {addr}[/bold cyan]", spinner="dots"):
        time.sleep(0.8)


def get_master_client():
    options = [
        ('grpc.max_receive_message_length', 128 * 1024 * 1024),
        ('grpc.max_send_message_length', 128 * 1024 * 1024),
    ]
    channel = grpc.insecure_channel(MASTER_ADDR, options=options)
    return pb_grpc.MasterStub(channel)
def get_chunk_client(addr: str):
    options = [
        ('grpc.max_receive_message_length', 128 * 1024 * 1024),
        ('grpc.max_send_message_length', 128 * 1024 * 1024),
    ]
    channel = grpc.insecure_channel(addr, options=options)
    return pb_grpc.ChunkServerStub(channel)


def cmd_put(local_path: str, remote_path: str):
    """Upload a file to FREE-FS"""
    if not os.path.exists(local_path):
        console.print(f"[bold red]✗ Local file not found: {local_path}[/bold red]")
        return False
    file_size = os.path.getsize(local_path)
    num_chunks = max(1, math.ceil(file_size / CHUNK_SIZE))
    console.print()
    console.print(Panel(
        f"[bold]Uploading[/bold] [cyan]{local_path}[/cyan] → [green]{remote_path}[/green]\n"
        f"[dim]Size: {fmt_size(file_size)}  |  Chunks: {num_chunks}  |  Replication: 3x[/dim]",
        title="[bold cyan]📤 FREE-FS PUT[/bold cyan]",
        border_style="cyan"
    ))
    master = get_master_client()
    

    with console.status("[bold cyan]Allocating chunks on master...[/bold cyan]", spinner="dots2"):
        try:
            resp = master.OpenFile(pb.OpenFileRequest(
                path=remote_path,
                mode="create",
                file_size=file_size
            ))
        except grpc.RpcError as e:
            console.print(f"[bold red]✗ Master error: {e.details()}[/bold red]")
            return False
    if not resp.success:
        console.print(f"[bold red]✗ {resp.error}[/bold red]")
        return False
    console.print(f"  [green]✓[/green] Allocated [bold]{len(resp.chunks)}[/bold] chunks across [bold]{len(set(a for c in resp.chunks for a in c.locations))}[/bold] servers")
    console.print()
    

    with Progress(
        SpinnerColumn("dots", style="bold cyan"),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=40, style="cyan", complete_style="bold green"),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        upload_task = progress.add_task("Uploading chunks", total=file_size)
        with open(local_path, 'rb') as f:
            for chunk_info in resp.chunks:
                chunk_data = f.read(CHUNK_SIZE)
                if not free_fs_chunk_data:
                    break
                

                success = False
                for addr in [chunk_info.primary] + list(chunk_info.locations):
                    if not addr or addr == chunk_info.primary and success:
                        continue
                    try:
                        cc = get_chunk_client(addr)
                        write_resp = cc.WriteChunk(pb.WriteChunkRequest(
                            chunk_id=chunk_info.chunk_id,
                            offset=0,
                            data=chunk_data,
                            version=1
                        ))
                        if write_resp.success:
                            success = True
                    except Exception:
                        pass
                if not success:
                    console.print(f"\n[bold red]✗ Failed to write chunk {chunk_info.chunk_id[:8]}[/bold red]")
                    return False
                progress.update(upload_task, advance=len(chunk_data))
    console.print()
    console.print(Panel(
        f"[bold green]✓ Upload complete![/bold green]\n"
        f"  Path:   [cyan]{remote_path}[/cyan]\n"
        f"  Size:   [yellow]{fmt_size(file_size)}[/yellow]\n"
        f"  Chunks: [blue]{len(resp.chunks)}[/blue]\n"
        f"  MD5:    [dim]{md5_file(local_path)}[/dim]",
        border_style="green"
    ))
    return True
def cmd_get(remote_path: str, local_path: str):
    """Download a file from FREE-FS"""
    console.print()
    console.print(Panel(
        f"[bold]Downloading[/bold] [green]{remote_path}[/green] → [cyan]{local_path}[/cyan]",
        title="[bold cyan]📥 FREE-FS GET[/bold cyan]",
        border_style="cyan"
    ))
    master = get_master_client()
    with console.status("[bold cyan]Fetching chunk locations...[/bold cyan]", spinner="dots2"):
        try:
            resp = master.OpenFile(pb.OpenFileRequest(path=remote_path, mode="r"))
        except grpc.RpcError as e:
            console.print(f"[bold red]✗ Master error: {e.details()}[/bold red]")
            return False
    if not resp.success:
        console.print(f"[bold red]✗ {resp.error}[/bold red]")
        return False
    chunks = sorted(resp.chunks, key=lambda c: c.index)
    file_size = resp.file_size
    console.print(f"  [green]✓[/green] Found [bold]{len(chunks)}[/bold] chunks  ({fmt_size(file_size)})")
    console.print()
    os.makedirs(os.path.dirname(os.path.abspath(local_path)), exist_ok=True)
    with Progress(
        SpinnerColumn("dots", style="bold cyan"),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=40, style="cyan", complete_style="bold green"),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        download_task = progress.add_task("Downloading chunks", total=max(file_size, 1))
        with open(local_path, 'wb') as f:
            for chunk_info in chunks:
                data = None
                

                for addr in [chunk_info.primary] + list(chunk_info.locations):
                    if not addr:
                        continue
                    try:
                        cc = get_chunk_client(addr)
                        read_resp = cc.ReadChunk(pb.ReadChunkRequest(
                            chunk_id=chunk_info.chunk_id
                        ))
                        if read_resp.success:
                            data = read_resp.data
                            break
                    except Exception:
                        continue
                if data is None:
                    console.print(f"\n[bold red]✗ Failed to read chunk {chunk_info.chunk_id[:8]}[/bold red]")
                    return False
                f.write(data)
                progress.update(download_task, advance=len(data))
    console.print()
    console.print(Panel(
        f"[bold green]✓ Download complete![/bold green]\n"
        f"  Saved to: [cyan]{local_path}[/cyan]\n"
        f"  Size:     [yellow]{fmt_size(os.path.getsize(local_path))}[/yellow]",
        border_style="green"
    ))
    return True
def cmd_ls(path: str = "/"):
    """List files"""
    master = get_master_client()
    with console.status("[bold cyan]Fetching directory listing...[/bold cyan]", spinner="dots"):
        try:
            resp = master.ListDirectory(pb.ListDirRequest(path=path))
        except grpc.RpcError as e:
            console.print(f"[bold red]✗ {e.details()}[/bold red]")
            return
    if not resp.success:
        

        try:
            fresp = master.ListFiles(pb.ListFilesRequest(pattern=""))
        except:
            console.print(f"[bold red]✗ {resp.error}[/bold red]")
            return
        _display_file_list(fresp.files, path)
        return
    console.print()
    console.print(Rule(f"[bold cyan]📁 {path}[/bold cyan]", style="cyan"))
    table = Table(box=box.ROUNDED, border_style="dim", show_header=True, header_style="bold cyan")
    table.add_column("Name", style="bold white", min_width=30)
    table.add_column("Type", style="dim", width=8)
    for entry in sorted(resp.entries):
        name = os.path.basename(entry.rstrip('/'))
        if entry.endswith('/'):
            table.add_row(f"[bold blue]📁 {name}/[/bold blue]", "DIR")
        else:
            table.add_row(f"[white]📄 {name}[/white]", "FILE")
    if not resp.entries:
        table.add_row("[dim]  (empty)[/dim]", "")
    console.print(table)
    console.print()
def _display_file_list(files, pattern=""):
    console.print()
    console.print(Rule(f"[bold cyan]📁 Files matching: {pattern or '*'}[/bold cyan]", style="cyan"))
    table = Table(box=box.ROUNDED, border_style="dim", show_header=True, header_style="bold cyan")
    table.add_column("Path", style="bold white", min_width=35)
    table.add_column("Size", style="yellow", justify="right", width=12)
    table.add_column("Chunks", style="blue", justify="center", width=8)
    table.add_column("Modified", style="dim", width=20)
    table.add_column("Owner", style="dim", width=12)
    for f in sorted(files, key=lambda x: x.path):
        mod_time = datetime.fromtimestamp(f.updated_at).strftime("%Y-%m-%d %H:%M") if f.updated_at else "—"
        table.add_row(
            f"📄 {f.path}",
            fmt_size(f.size),
            str(f.chunk_count),
            mod_time,
            f.owner or "free-fs-user"
        )
    if not files:
        table.add_row("[dim]  (no files)[/dim]", "", "", "", "")
    console.print(table)
    console.print(f"  [dim]{len(files)} file(s)[/dim]")
    console.print()
def cmd_rm(path: str):
    """Delete a file"""
    if not Confirm.ask(f"  [bold red]Delete[/bold red] [cyan]{path}[/cyan]?"):
        console.print("[dim]  Cancelled.[/dim]")
        return
    master = get_master_client()
    with console.status(f"[bold red]Deleting {path}...[/bold red]", spinner="dots"):
        try:
            resp = master.DeleteFile(pb.DeleteFileRequest(path=path))
        except grpc.RpcError as e:
            console.print(f"[bold red]✗ {e.details()}[/bold red]")
            return
    if resp.success:
        console.print(f"  [bold green]✓ Deleted:[/bold green] [cyan]{path}[/cyan]")
    else:
        console.print(f"  [bold red]✗ {resp.error}[/bold red]")
def cmd_stat(path: str):
    """Show file info"""
    master = get_master_client()
    with console.status("[bold cyan]Fetching file info...[/bold cyan]", spinner="dots"):
        try:
            resp = master.GetFileInfo(pb.GetFileInfoRequest(path=path))
        except grpc.RpcError as e:
            console.print(f"[bold red]✗ {e.details()}[/bold red]")
            return
    if not resp.success:
        console.print(f"  [bold red]✗ {resp.error}[/bold red]")
        return
    f = resp.info
    created = datetime.fromtimestamp(f.created_at).strftime("%Y-%m-%d %H:%M:%S") if f.created_at else "—"
    updated = datetime.fromtimestamp(f.updated_at).strftime("%Y-%m-%d %H:%M:%S") if f.updated_at else "—"
    console.print()
    console.print(Panel(
        f"[bold cyan]Path:[/bold cyan]       {f.path}\n"
        f"[bold cyan]Size:[/bold cyan]       [yellow]{fmt_size(f.size)}[/yellow] ({f.size:,} bytes)\n"
        f"[bold cyan]Chunks:[/bold cyan]     [blue]{f.chunk_count}[/blue] × 64MB\n"
        f"[bold cyan]Owner:[/bold cyan]      {f.owner}\n"
        f"[bold cyan]Created:[/bold cyan]    [dim]{created}[/dim]\n"
        f"[bold cyan]Modified:[/bold cyan]   [dim]{updated}[/dim]",
        title=f"[bold]📄 {os.path.basename(f.path)}[/bold]",
        border_style="cyan"
    ))
def cmd_mkdir(path: str):
    """Create directory"""
    master = get_master_client()
    with console.status(f"[bold cyan]Creating directory {path}...[/bold cyan]", spinner="dots"):
        try:
            resp = master.CreateDirectory(pb.CreateDirRequest(path=path))
        except grpc.RpcError as e:
            console.print(f"[bold red]✗ {e.details()}[/bold red]")
            return
    if resp.success:
        console.print(f"  [bold green]✓[/bold green] Created directory: [cyan]{path}[/cyan]")
    else:
        console.print(f"  [bold red]✗ {resp.error}[/bold red]")
def cmd_mv(src: str, dst: str):
    """Move/rename a file"""
    master = get_master_client()
    with console.status(f"[bold cyan]Moving {src} → {dst}...[/bold cyan]", spinner="dots"):
        try:
            resp = master.MoveFile(pb.MoveFileRequest(src=src, dst=dst))
        except grpc.RpcError as e:
            console.print(f"[bold red]✗ {e.details()}[/bold red]")
            return
    if resp.success:
        console.print(f"  [bold green]✓[/bold green] Moved [cyan]{src}[/cyan] → [cyan]{dst}[/cyan]")
    else:
        console.print(f"  [bold red]✗ {resp.error}[/bold red]")
def cmd_status():
    """Show cluster status with animated dashboard"""
    master = get_master_client()
    with console.status("[bold cyan]Fetching cluster status...[/bold cyan]", spinner="arc"):
        try:
            resp = master.GetClusterStatus(pb.ClusterStatusRequest())
        except grpc.RpcError as e:
            console.print(f"[bold red]✗ Cannot reach master: {e.details()}[/bold red]")
            console.print(f"  [dim]Make sure master is running at {MASTER_ADDR}[/dim]")
            return
    console.print()
    console.print(Rule("[bold cyan]🖥  FREE-FS CLUSTER STATUS[/bold cyan]", style="cyan"))
    console.print()
    

    alive_count = sum(1 for s in resp.servers if s.alive)
    dead_count = len(resp.servers) - alive_count
    used_pct = (resp.used_capacity / resp.total_capacity * 100) if resp.total_capacity > 0 else 0
    free = resp.total_capacity - resp.used_capacity
    cards = [
        Panel(
            f"[bold {'green' if alive_count > 0 else 'red'}]{alive_count}[/bold {'green' if alive_count > 0 else 'red'}] alive\n[dim]{dead_count} offline[/dim]",
            title="[bold]ChunkServers[/bold]", border_style="green" if alive_count > 0 else "red", width=22
        ),
        Panel(
            f"[bold yellow]{resp.total_files}[/bold yellow] files\n[dim]{resp.total_chunks} chunks[/dim]",
            title="[bold]Data[/bold]", border_style="yellow", width=22
        ),
        Panel(
            f"[bold cyan]{fmt_size(resp.total_capacity)}[/bold cyan] total\n[dim]{fmt_size(free)} free[/dim]",
            title="[bold]Storage[/bold]", border_style="cyan", width=22
        ),
        Panel(
            f"[bold {'red' if used_pct > 80 else 'green'}]{used_pct:.1f}%[/bold {'red' if used_pct > 80 else 'green'}] used\n{_mini_bar(used_pct)}",
            title="[bold]Usage[/bold]", border_style="blue", width=22
        ),
    ]
    console.print(Columns(cards, equal=True))
    console.print()
    

    if resp.servers:
        table = Table(
            title="[bold]Chunk Servers[/bold]",
            box=box.ROUNDED,
            border_style="dim",
            show_header=True,
            header_style="bold cyan"
        )
        table.add_column("ID", style="bold", width=12)
        table.add_column("Address", style="cyan", width=22)
        table.add_column("Status", justify="center", width=10)
        table.add_column("Chunks", justify="right", style="blue", width=8)
        table.add_column("CPU", justify="right", width=8)
        table.add_column("RAM", justify="right", width=8)
        table.add_column("Storage", justify="right", width=16)
        table.add_column("Last Beat", style="dim", width=14)
        for s in sorted(resp.servers, key=lambda x: x.server_id):
            status = "[bold green]● ALIVE[/bold green]" if s.alive else "[bold red]✗ DEAD[/bold red]"
            used_s = s.capacity - s.available
            storage_bar = f"{fmt_size(used_s)} / {fmt_size(s.capacity)}"
            last_beat = ""
            if s.last_heartbeat:
                ago = int(time.time()) - s.last_heartbeat
                last_beat = f"{ago}s ago"
            table.add_row(
                s.server_id[:10],
                s.address,
                status,
                str(s.chunk_count),
                f"{s.cpu_usage:.1f}%",
                f"{s.mem_usage:.1f}%",
                storage_bar,
                last_beat
            )
        console.print(table)
    else:
        console.print(Panel(
            "[bold yellow]⚠  No chunk servers registered yet[/bold yellow]\n"
            "[dim]Start chunk servers to begin storing files[/dim]",
            border_style="yellow"
        ))
    console.print()
def cmd_demo():
    """Run an interactive demo of all FREE-FS features"""
    show_banner()
    console.print(Panel(
        "[bold cyan]FREE-FS Demo[/bold cyan] — This will demonstrate all major features:\n\n"
        "  [bold]1.[/bold] Check cluster status\n"
        "  [bold]2.[/bold] Create directories\n"
        "  [bold]3.[/bold] Upload files (small + large)\n"
        "  [bold]4.[/bold] List files\n"
        "  [bold]5.[/bold] Download & verify files\n"
        "  [bold]6.[/bold] Show file metadata\n"
        "  [bold]7.[/bold] Move files\n"
        "  [bold]8.[/bold] Delete files\n"
        "  [bold]9.[/bold] Cluster health check",
        title="[bold]🎬 DEMO MODE[/bold]",
        border_style="cyan"
    ))
    console.print()
    if not Confirm.ask("  [bold]Start demo?[/bold]"):
        return
    steps = [
        ("Cluster Status",       lambda: cmd_status()),
        ("Create /demo directory", lambda: cmd_mkdir("/demo")),
        ("Create /demo/docs",    lambda: cmd_mkdir("/demo/docs")),
        ("List root /",          lambda: cmd_ls("/")),
        ("Upload small file",    lambda: _demo_upload_small()),
        ("Upload large file",    lambda: _demo_upload_large()),
        ("List /demo",           lambda: cmd_ls("/demo")),
        ("File stat",            lambda: cmd_stat("/demo/hello.txt")),
        ("Download file",        lambda: _demo_download()),
        ("Move file",            lambda: cmd_mv("/demo/hello.txt", "/demo/docs/hello_moved.txt")),
        ("List /demo/docs",      lambda: cmd_ls("/demo/docs")),
        ("Delete file",          lambda: _demo_delete()),
        ("Final cluster status", lambda: cmd_status()),
    ]
    for i, (title, fn) in enumerate(steps):
        console.print()
        console.print(Rule(f"[bold cyan]Step {i+1}/{len(steps)}: {title}[/bold cyan]", style="cyan"))
        time.sleep(0.3)
        try:
            fn()
        except Exception as e:
            console.print(f"  [bold red]Step failed: {e}[/bold red]")
        time.sleep(0.5)
    console.print()
    console.print(Panel(
        "[bold green]🎉 Demo Complete![/bold green]\n\n"
        "You've seen all FREE-FS features in action.\n"
        "Run [bold cyan]free-fs --help[/bold cyan] for all available commands.",
        border_style="green"
    ))
def _demo_upload_small():
    

    tmp = "/tmp/gfs_demo_hello.txt"
    with open(tmp, 'w') as f:
        f.write("Hello from FREE-FS! 🚀\n" * 1000)
        f.write(f"Timestamp: {datetime.now()}\n")
    cmd_put(tmp, "/demo/hello.txt")
    os.unlink(tmp)
def _demo_upload_large():
    tmp = "/tmp/gfs_demo_large.bin"
    console.print(f"  [dim]Creating 5MB test file...[/dim]")
    with open(tmp, 'wb') as f:
        f.write(os.urandom(5 * 1024 * 1024))
    cmd_put(tmp, "/demo/large_file.bin")
    os.unlink(tmp)
def _demo_download():
    out = "/tmp/gfs_demo_downloaded.txt"
    cmd_get("/demo/hello.txt", out)
    if os.path.exists(out):
        with open(out) as f:
            lines = f.readlines()
        console.print(f"  [dim]Downloaded {len(lines)} lines. First: {lines[0].strip()[:60]}[/dim]")
        os.unlink(out)
def _demo_delete():
    master = get_master_client()
    try:
        r = master.DeleteFile(pb.DeleteFileRequest(path="/demo/large_file.bin"))
        if r.success:
            console.print("  [bold green]✓ Deleted /demo/large_file.bin[/bold green]")
    except:
        pass


def cmd_shell():
    """Interactive FREE-FS shell"""
    show_banner()
    console.print(Panel(
        "[bold cyan]FREE-FS Interactive Shell[/bold cyan]\n"
        "[dim]Type [bold]help[/bold] for commands, [bold]exit[/bold] to quit[/dim]",
        border_style="cyan"
    ))
    console.print()
    while True:
        try:
            cmd_line = Prompt.ask("[bold cyan]free-fs[/bold cyan] [dim]›[/dim]").strip()
        except (EOFError, KeyboardInterrupt):
            console.print("\n[dim]Bye![/dim]")
            break
        if not cmd_line:
            continue
        parts = cmd_line.split()
        cmd = parts[0].lower()
        args = parts[1:]
        try:
            if cmd in ("exit", "quit", "q"):
                console.print("[dim]Goodbye! 👋[/dim]")
                break
            elif cmd == "help":
                _shell_help()
            elif cmd == "status":
                cmd_status()
            elif cmd == "ls":
                cmd_ls(args[0] if args else "/")
            elif cmd == "put" and len(args) == 2:
                cmd_put(args[0], args[1])
            elif cmd == "get" and len(args) == 2:
                cmd_get(args[0], args[1])
            elif cmd == "rm" and args:
                cmd_rm(args[0])
            elif cmd == "stat" and args:
                cmd_stat(args[0])
            elif cmd == "mkdir" and args:
                cmd_mkdir(args[0])
            elif cmd == "mv" and len(args) == 2:
                cmd_mv(args[0], args[1])
            elif cmd == "demo":
                cmd_demo()
            else:
                console.print(f"  [bold red]Unknown command:[/bold red] {cmd_line}  [dim](type help)[/dim]")
        except grpc.RpcError as e:
            console.print(f"  [bold red]gRPC error: {e.details()}[/bold red]")
        except Exception as e:
            console.print(f"  [bold red]Error: {e}[/bold red]")
def _shell_help():
    table = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
    table.add_column("Command", style="bold cyan", width=30)
    table.add_column("Description", style="white")
    cmds = [
        ("put <local> <remote>",  "Upload file to FREE-FS"),
        ("get <remote> <local>",  "Download file from FREE-FS"),
        ("ls [path]",             "List directory contents"),
        ("rm <path>",             "Delete a file"),
        ("stat <path>",           "Show file metadata"),
        ("mkdir <path>",          "Create directory"),
        ("mv <src> <dst>",        "Move/rename file"),
        ("status",                "Show cluster status"),
        ("demo",                  "Run interactive demo"),
        ("exit",                  "Quit"),
    ]
    for cmd, desc in cmds:
        table.add_row(cmd, desc)
    console.print(Panel(table, title="[bold]Available Commands[/bold]", border_style="dim"))


def fmt_size(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    elif n < 1024**2:
        return f"{n/1024:.1f} KB"
    elif n < 1024**3:
        return f"{n/1024**2:.1f} MB"
    else:
        return f"{n/1024**3:.2f} GB"
def md5_file(path: str) -> str:
    h = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()
def _mini_bar(pct: float, width=12) -> str:
    filled = int(pct / 100 * width)
    color = "green" if pct < 60 else "yellow" if pct < 80 else "red"
    bar = "█" * filled + "░" * (width - filled)
    return f"[{color}]{bar}[/{color}]"


def main():
    global MASTER_ADDR
    parser = argparse.ArgumentParser(
        description="FREE-FS - Google File System Replica CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--master", "-m", default=MASTER_ADDR,
                        help=f"Master address (default: {MASTER_ADDR})")
    parser.add_argument("--no-banner", action="store_true")
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    

    p = sub.add_parser("put", help="Upload file")
    p.add_argument("local", help="Local file path")
    p.add_argument("remote", help="Remote FREE-FS path")
    

    p = sub.add_parser("get", help="Download file")
    p.add_argument("remote", help="Remote FREE-FS path")
    p.add_argument("local", help="Local output path")
    

    p = sub.add_parser("ls", help="List files/directories")
    p.add_argument("path", nargs="?", default="/", help="Path to list")
    

    p = sub.add_parser("rm", help="Delete file")
    p.add_argument("path", help="File path")
    

    p = sub.add_parser("stat", help="File info")
    p.add_argument("path", help="File path")
    

    p = sub.add_parser("mkdir", help="Create directory")
    p.add_argument("path", help="Directory path")
    

    p = sub.add_parser("mv", help="Move/rename file")
    p.add_argument("src", help="Source path")
    p.add_argument("dst", help="Destination path")
    

    sub.add_parser("status", help="Cluster status")
    

    sub.add_parser("demo", help="Run full demo")
    

    sub.add_parser("shell", help="Interactive shell")
    args = parser.parse_args()
    MASTER_ADDR = args.master
    if not args.no_banner and args.command not in ("status",):
        show_banner()
    try:
        if args.command == "put":
            cmd_put(args.local, args.remote)
        elif args.command == "get":
            cmd_get(args.remote, args.local)
        elif args.command == "ls":
            cmd_ls(args.path)
        elif args.command == "rm":
            cmd_rm(args.path)
        elif args.command == "stat":
            cmd_stat(args.path)
        elif args.command == "mkdir":
            cmd_mkdir(args.path)
        elif args.command == "mv":
            cmd_mv(args.src, args.dst)
        elif args.command == "status":
            cmd_status()
        elif args.command == "demo":
            cmd_demo()
        elif args.command == "shell":
            cmd_shell()
        else:
            cmd_shell()
    except KeyboardInterrupt:
        console.print("\n[dim]Interrupted.[/dim]")
if __name__ == "__main__":
    main()
