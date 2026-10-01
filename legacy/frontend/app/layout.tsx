import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import { Providers } from "./providers";
import { Navigation } from "@/components/shared/navigation";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "StockMind - AI 매매 신호 대시보드",
  description: "AI 기반 주식 매매 신호 및 감성 분석 플랫폼",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="ko" suppressHydrationWarning>
      <body className={inter.className}>
        <Providers>
          <Navigation />
          {children}
        </Providers>
      </body>
    </html>
  );
}
