"use client";

import dynamic from "next/dynamic";
import Link from "next/link";

const MapInner = dynamic(() => import("./MapInner").then((m) => m.MapInner), {
  ssr: false,
  loading: () => (
    <div className="map-loading">
      <div className="map-loading__spinner" />
      <span className="map-loading__text">Loading map...</span>
      <style jsx>{`
        .map-loading {
          width: 100%;
          height: 100%;
          display: flex;
          flex-direction: column;
          align-items: center;
          justify-content: center;
          background: #0d0f12;
          gap: 16px;
        }
        .map-loading__spinner {
          width: 40px;
          height: 40px;
          border: 3px solid rgba(196, 120, 92, 0.2);
          border-top-color: #c4785c;
          border-radius: 50%;
          animation: spin 1s linear infinite;
        }
        @keyframes spin {
          to { transform: rotate(360deg); }
        }
        .map-loading__text {
          font-size: 0.8rem;
          color: #6b7a90;
          letter-spacing: 0.05em;
        }
      `}</style>
    </div>
  ),
});

export default function MapPage() {
  return (
    <div className="map-page">
      {/* Top Navigation Bar */}
      <nav className="map-nav">
        <Link href="/" className="map-nav__logo">
          <span className="map-nav__logo-icon">◆</span>
          <span className="map-nav__logo-text">Italia Immobiliare</span>
        </Link>
        <div className="map-nav__links">
          <Link href="/map" className="map-nav__link map-nav__link--active">
            Map
          </Link>
          <Link href="/rankings" className="map-nav__link">
            Rankings
          </Link>
          <Link href="/methodology" className="map-nav__link">
            Methodology
          </Link>
        </div>
      </nav>

      {/* Map Container */}
      <main className="map-main">
        <MapInner />
      </main>

      <style jsx>{`
        .map-page {
          height: 100vh;
          display: flex;
          flex-direction: column;
          background: #0d0f12;
          overflow: hidden;
        }

        .map-nav {
          height: 56px;
          display: flex;
          align-items: center;
          justify-content: space-between;
          padding: 0 24px;
          background: linear-gradient(180deg,
            rgba(22, 25, 32, 0.98) 0%,
            rgba(13, 15, 18, 0.95) 100%
          );
          border-bottom: 1px solid rgba(255, 255, 255, 0.06);
          z-index: 100;
        }

        .map-nav__logo {
          display: flex;
          align-items: center;
          gap: 10px;
          text-decoration: none;
        }

        .map-nav__logo-icon {
          font-size: 1.25rem;
          color: #c4785c;
        }

        .map-nav__logo-text {
          font-family: 'Cormorant Garamond', Georgia, serif;
          font-size: 1.1rem;
          font-weight: 600;
          color: #f0f2f5;
          letter-spacing: 0.02em;
        }

        .map-nav__links {
          display: flex;
          align-items: center;
          gap: 8px;
        }

        .map-nav__link {
          padding: 8px 16px;
          font-size: 0.8rem;
          font-weight: 500;
          color: #8b9bb4;
          text-decoration: none;
          border-radius: 8px;
          transition: all 0.2s ease;
        }

        .map-nav__link:hover {
          color: #d0d7e2;
          background: rgba(255, 255, 255, 0.04);
        }

        .map-nav__link--active {
          color: #f0f2f5;
          background: rgba(196, 120, 92, 0.15);
        }

        .map-main {
          flex: 1;
          position: relative;
          min-height: 0;
        }
      `}</style>
    </div>
  );
}
