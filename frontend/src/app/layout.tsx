import type { Metadata } from "next";
import "./globals.css";
import Providers from "./providers";
import { ThemeProvider } from "@/components/ThemeProvider";
import { ThemeToggle } from "@/components/ThemeToggle";
import { SyncStatus } from "@/components/SyncStatus";
import HeaderNav from "@/components/HeaderNav";
import MobileNav from "@/components/MobileNav";
import HomeLabel from "@/components/HomeLabel";
import FooterSocials from "@/components/FooterSocials";
import { ScrollToTop } from "@/components/ScrollToTop";
import Link from "next/link";
import config from "../config.json";

export const metadata: Metadata = {
  metadataBase: new URL('https://radiantexplorer.com'),
  title: {
    default: config.site.title,
    template: `%s | ${config.site.title}`,
  },
  description: config.site.description,
  openGraph: {
    title: config.site.title,
    description: config.site.description,
    url: 'https://radiantexplorer.com',
    siteName: config.site.title,
    images: [
      {
        url: '/og-image.png',
        width: 1200,
        height: 630,
        alt: 'Radiant Explorer',
      },
    ],
    locale: 'en_US',
    type: 'website',
  },
  twitter: {
    card: 'summary_large_image',
    title: config.site.title,
    description: config.site.description,
    images: ['/og-image.png'],
    creator: '@neuraiproject',
  },
  icons: {
    icon: config.site.logoPath || '/radiant-logo.png',
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className={`font-sans antialiased`}>
        <Providers>
          <ThemeProvider
            attribute="class"
            defaultTheme="dark"
            disableTransitionOnChange
          >
            <div className="flex flex-col min-h-screen bg-background text-foreground">
              <header className="sticky top-0 z-50 w-full border-b border-border bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
                <div className="container max-w-7xl mx-auto flex h-16 items-center flex justify-between px-4">
                  <Link href="/" className="mr-8 flex items-center space-x-2 group">
                    <img
                      src={config.site.logoPath || "/neurai-logo.png"}
                      alt={config.site.logoAlt || `${config.site.coinName} Logo`}
                      className="w-8 h-8 rounded-full"
                    />
                    <HomeLabel />
                  </Link>
                  <HeaderNav className="hidden lg:flex" />
                  <nav className="flex items-center space-x-3">
                    <SyncStatus />
                    <div className="hidden lg:block">
                      <ThemeToggle />
                    </div>
                    <MobileNav />
                  </nav>
                </div>
              </header>
              <main className="flex-1 container max-w-7xl mx-auto py-8 px-4">{children}</main>
              <footer className="border-t border-border py-8 bg-muted/30">
                <div className="container max-w-7xl mx-auto flex flex-col items-center gap-4 text-center text-sm text-muted-foreground">
                  <p>© {new Date().getFullYear()} Radiant.</p>
                  <FooterSocials />
                </div>
              </footer>
            </div>
            <ScrollToTop />

          </ThemeProvider>
        </Providers>
      </body >
    </html >
  );
}
