import type { AppProps } from 'next/app'
import Link from 'next/link'
import '../styles/globals.css'

export default function App({ Component, pageProps }: AppProps) {
  return (
    <div className="min-h-screen bg-gray-50">
      <nav className="bg-gray-900 text-white px-6 py-3 flex items-center gap-8">
        <Link href="/" className="text-lg font-bold tracking-tight hover:text-green-400 transition-colors">
          SundaySignals
        </Link>
        <Link href="/" className="text-sm text-gray-300 hover:text-white transition-colors">
          Player Explorer
        </Link>
        <Link href="/accuracy" className="text-sm text-gray-300 hover:text-white transition-colors">
          Model Accuracy
        </Link>
      </nav>
      <main className="max-w-7xl mx-auto px-4 py-8">
        <Component {...pageProps} />
      </main>
    </div>
  )
}
