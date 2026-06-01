import { useEffect, useState, type ReactNode } from "react";
import { Links, Meta, Outlet, Scripts, ScrollRestoration } from "react-router";
import { ThemeProvider } from "next-themes";
import { Toaster } from "~/components/ui/sonner";
import { SidebarInset, SidebarProvider, SidebarTrigger } from "~/components/ui/sidebar";
import { AppSidebar } from "~/components/app/AppSidebar";
import { api } from "~/lib/api";
import "./app.css";

export function Layout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <meta charSet="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>CorpScout</title>
        <Meta />
        <Links />
      </head>
      <body className="min-h-screen bg-background antialiased">
        <ThemeProvider attribute="class" defaultTheme="system" enableSystem disableTransitionOnChange>
          {children}
          <Toaster />
        </ThemeProvider>
        <ScrollRestoration />
        <Scripts />
      </body>
    </html>
  );
}

function AppShell() {
  const [pendingReview, setPendingReview] = useState<number | undefined>();

  useEffect(() => {
    api.getStats()
      .then((s) => setPendingReview(s.pending_review))
      .catch(() => {});
  }, []);

  return (
    <SidebarProvider>
      <AppSidebar pendingReview={pendingReview} />
      <SidebarInset>
        <header className="flex h-12 shrink-0 items-center gap-2 border-b px-4">
          <SidebarTrigger className="-ml-1" />
        </header>
        <main className="flex min-h-0 flex-1 flex-col p-4">
          <Outlet />
        </main>
      </SidebarInset>
    </SidebarProvider>
  );
}

export default function App() {
  return <AppShell />;
}
