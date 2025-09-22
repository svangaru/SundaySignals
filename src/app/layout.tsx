
export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="antialiased max-w-6xl mx-auto p-4">
        <header className="py-4"><h1>Fantasy FF</h1></header>
        <main>{children}</main>
      </body>
    </html>
  );
}
