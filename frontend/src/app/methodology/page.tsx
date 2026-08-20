"use client";

import Link from "next/link";

export default function MethodologyPage() {
  return (
    <div className="methodology-page">
      {/* Navigation */}
      <nav className="nav">
        <Link
          href="/"
          className="nav__logo"
          style={{ display: 'flex', flexDirection: 'row', alignItems: 'center', gap: '12px', textDecoration: 'none' }}
        >
          <img
            src="/supersavvytravelers.png"
            alt="Super Savvy Travelers"
            style={{ height: '36px', width: 'auto', objectFit: 'contain', flexShrink: 0 }}
          />
          <span className="nav__logo-text">Super Savvy Travelers</span>
        </Link>
        <div className="nav__links">
          <Link href="/map" className="nav__link">Map</Link>
          <Link href="/rankings" className="nav__link">Rankings</Link>
          <Link href="/methodology" className="nav__link nav__link--active">Methodology</Link>
        </div>
      </nav>

      {/* Content */}
      <main className="main">
        <div className="content">
          <header className="header">
            <h1 className="header__title">Methodology</h1>
            <p className="header__subtitle">
              Data sources, calculations, and limitations
            </p>
          </header>

          {/* Data Sources Section */}
          <section className="section">
            <h2 className="section__title">Data Sources</h2>

            <div className="card">
              <h3 className="card__title">OMI (Osservatorio del Mercato Immobiliare)</h3>
              <p className="card__text">
                The primary data source for property values and rental prices is the OMI database,
                published by the <strong>Agenzia delle Entrate</strong> (Italian Revenue Agency).
                OMI provides official real estate market data for all Italian municipalities,
                updated twice yearly (H1 and H2).
              </p>
              <div className="card__details">
                <div className="detail">
                  <span className="detail__label">Coverage</span>
                  <span className="detail__value">7,900+ municipalities</span>
                </div>
                <div className="detail">
                  <span className="detail__label">Update Frequency</span>
                  <span className="detail__value">Semi-annual (H1/H2)</span>
                </div>
                <div className="detail">
                  <span className="detail__label">Historical Data</span>
                  <span className="detail__value">2016 – Present</span>
                </div>
              </div>
              <p className="card__text card__text--note">
                OMI divides each municipality into homogeneous zones (e.g., central, semi-central,
                peripheral, suburban) and provides min/max price ranges for different property
                types and conditions.
              </p>
            </div>

            <div className="card">
              <h3 className="card__title">ISTAT (Italian National Institute of Statistics)</h3>
              <p className="card__text">
                Demographic data is sourced from ISTAT, providing population statistics,
                foreign resident ratios, and population growth rates at the municipality level.
              </p>
              <div className="card__details">
                <div className="detail">
                  <span className="detail__label">Data Type</span>
                  <span className="detail__value">Demographics, population</span>
                </div>
                <div className="detail">
                  <span className="detail__label">Update Frequency</span>
                  <span className="detail__value">Annual</span>
                </div>
              </div>
            </div>
          </section>

          {/* Metrics Section */}
          <section className="section">
            <h2 className="section__title">Metrics Explained</h2>

            <div className="card">
              <h3 className="card__title">Property Value (€/m²)</h3>
              <p className="card__text">
                The midpoint value between OMI's minimum and maximum price ranges for residential
                properties in "normale" (standard) condition. Values are aggregated at the municipality
                level by averaging across all zones, weighted by the number of data points in each zone.
              </p>
              <div className="formula">
                <code>Value = (OMI_min + OMI_max) / 2</code>
              </div>
            </div>

            <div className="card">
              <h3 className="card__title">Gross Rental Yield (%)</h3>
              <p className="card__text">
                Annual rental income as a percentage of property value, before expenses.
                Calculated from OMI rental data (monthly rent per m²) and property values.
              </p>
              <div className="formula">
                <code>Gross Yield = (Monthly Rent × 12 / Property Value) × 100</code>
              </div>
              <p className="card__text card__text--note">
                This is a <em>gross</em> yield that does not account for vacancy, maintenance,
                taxes, or management costs. Net yields are typically 1-2% lower.
              </p>
            </div>

            <div className="card">
              <h3 className="card__title">Price Change (%)</h3>
              <p className="card__text">
                The percentage change in property values over the selected time period.
                For multi-year periods, the change is <em>annualized</em> using compound annual growth rate (CAGR).
              </p>
              <div className="formula">
                <code>Annualized Change = ((Current / Past)^(1/years) - 1) × 100</code>
              </div>
            </div>

            <div className="card">
              <h3 className="card__title">Condition Premium (%)</h3>
              <p className="card__text">
                The price premium for properties in "ottimo" (excellent) condition versus
                "normale" (standard) condition. Higher premiums may indicate markets where
                renovations are valued or where housing stock quality varies significantly.
              </p>
              <div className="formula">
                <code>Premium = ((Ottimo Price - Normale Price) / Normale Price) × 100</code>
              </div>
            </div>

            <div className="card">
              <h3 className="card__title">Price Variance (%)</h3>
              <p className="card__text">
                The spread between minimum and maximum prices relative to the midpoint,
                indicating price heterogeneity within a municipality. Higher variance suggests
                more diverse neighborhoods or property types.
              </p>
              <div className="formula">
                <code>Variance = ((Max - Min) / Mid) × 100</code>
              </div>
            </div>

            <div className="card">
              <h3 className="card__title">Foreign Resident Ratio (%)</h3>
              <p className="card__text">
                The percentage of registered residents who are foreign nationals, from ISTAT
                demographic data. This can indicate international appeal or established
                expat communities.
              </p>
            </div>
          </section>

          {/* Averaging Section */}
          <section className="section">
            <h2 className="section__title">Data Averaging</h2>

            <div className="card">
              <h3 className="card__title">Multi-Semester Averaging</h3>
              <p className="card__text">
                To reduce noise from semester-to-semester fluctuations, values can be averaged
                across multiple periods. The available options are:
              </p>
              <ul className="card__list">
                <li><strong>6 months (1 semester)</strong> — Latest data only, most current but potentially volatile</li>
                <li><strong>1 year (2 semesters)</strong> — Smooths seasonal variations</li>
                <li><strong>2 years (4 semesters)</strong> — Default; balances recency with stability</li>
                <li><strong>4 years (8 semesters)</strong> — Most stable, useful for long-term trends</li>
              </ul>
            </div>

            <div className="card">
              <h3 className="card__title">Zone Aggregation</h3>
              <p className="card__text">
                OMI data is provided at the zone level (multiple zones per municipality).
                Municipality-level values are calculated by averaging across all residential
                zones within each municipality. Zones with incomplete data are excluded from calculations.
              </p>
            </div>
          </section>

          {/* Limitations Section */}
          <section className="section">
            <h2 className="section__title">Limitations & Considerations</h2>

            <div className="card card--warning">
              <h3 className="card__title">Data Coverage</h3>
              <p className="card__text">
                Not all municipalities have complete OMI data for all time periods.
                Small municipalities or those with limited real estate activity may have
                gaps in coverage. The platform displays data only where available.
              </p>
            </div>

            <div className="card card--warning">
              <h3 className="card__title">Price Ranges vs. Transaction Prices</h3>
              <p className="card__text">
                OMI values represent <em>estimated market ranges</em>, not actual transaction prices.
                Real transactions may fall outside these ranges depending on specific property
                characteristics, negotiation, and market conditions.
              </p>
            </div>

            <div className="card card--warning">
              <h3 className="card__title">Rental Yield Assumptions</h3>
              <p className="card__text">
                Gross yields assume 100% occupancy and do not account for:
              </p>
              <ul className="card__list">
                <li>Vacancy periods between tenants</li>
                <li>Property taxes (IMU, TASI) and income taxes</li>
                <li>Maintenance and repair costs</li>
                <li>Property management fees</li>
                <li>Insurance and condominium fees</li>
              </ul>
              <p className="card__text">
                Actual net yields after expenses are typically 1-3% lower than gross yields shown.
              </p>
            </div>

            <div className="card card--warning">
              <h3 className="card__title">Historical Comparisons</h3>
              <p className="card__text">
                Price change calculations require data availability in both the current and
                comparison periods. Municipalities without historical data will not show
                price change metrics.
              </p>
            </div>
          </section>

          {/* Data Sources Links */}
          <section className="section">
            <h2 className="section__title">Official Sources</h2>
            <div className="sources">
              <a
                href="https://www.agenziaentrate.gov.it/portale/web/guest/schede/fabbricatiterreni/omi"
                target="_blank"
                rel="noopener noreferrer"
                className="source-link"
              >
                <span className="source-link__icon">🏛️</span>
                <span className="source-link__text">
                  <strong>OMI Database</strong>
                  <span>Agenzia delle Entrate</span>
                </span>
                <span className="source-link__arrow">→</span>
              </a>
              <a
                href="https://www.istat.it/"
                target="_blank"
                rel="noopener noreferrer"
                className="source-link"
              >
                <span className="source-link__icon">📊</span>
                <span className="source-link__text">
                  <strong>ISTAT</strong>
                  <span>Italian National Institute of Statistics</span>
                </span>
                <span className="source-link__arrow">→</span>
              </a>
            </div>
          </section>
        </div>
      </main>

      <style jsx>{`
        .methodology-page {
          min-height: 100vh;
          background: #0d0f12;
          color: #f0f2f5;
        }

        /* Navigation */
        .nav {
          display: flex;
          align-items: center;
          justify-content: space-between;
          padding: 0 32px;
          height: 64px;
          background: linear-gradient(180deg, rgba(22, 25, 32, 0.98) 0%, rgba(13, 15, 18, 0.95) 100%);
          border-bottom: 1px solid rgba(255, 255, 255, 0.06);
        }

        .nav__logo {
          display: flex;
          flex-direction: row;
          align-items: center;
          gap: 12px;
          text-decoration: none;
          white-space: nowrap;
        }

        .nav__logo-text {
          font-size: 1.1rem;
          font-weight: 600;
          color: #f0f2f5;
        }

        .nav__links {
          display: flex;
          gap: 8px;
        }

        .nav__link {
          padding: 8px 16px;
          font-size: 0.8rem;
          font-weight: 500;
          color: #8b9bb4;
          text-decoration: none;
          border-radius: 8px;
          transition: all 0.2s;
        }

        .nav__link:hover {
          color: #d0d7e2;
          background: rgba(255, 255, 255, 0.04);
        }

        .nav__link--active {
          color: #f0f2f5;
          background: rgba(196, 120, 92, 0.15);
        }

        /* Main Content */
        .main {
          padding: 48px 32px 80px;
        }

        .content {
          max-width: 800px;
          margin: 0 auto;
        }

        /* Header */
        .header {
          margin-bottom: 48px;
        }

        .header__title {
          font-family: 'Cormorant Garamond', Georgia, serif;
          font-size: 2.5rem;
          font-weight: 600;
          margin: 0 0 12px;
          color: #f0f2f5;
        }

        .header__subtitle {
          font-size: 1rem;
          color: #6b7a90;
          margin: 0;
        }

        /* Sections */
        .section {
          margin-bottom: 48px;
        }

        .section__title {
          font-size: 1.25rem;
          font-weight: 600;
          color: #d0d7e2;
          margin: 0 0 20px;
          padding-bottom: 12px;
          border-bottom: 1px solid rgba(255, 255, 255, 0.08);
        }

        /* Cards */
        .card {
          background: rgba(22, 25, 32, 0.6);
          border: 1px solid rgba(255, 255, 255, 0.06);
          border-radius: 12px;
          padding: 24px;
          margin-bottom: 16px;
        }

        .card--warning {
          border-left: 3px solid rgba(251, 191, 36, 0.5);
        }

        .card__title {
          font-size: 1rem;
          font-weight: 600;
          color: #f0f2f5;
          margin: 0 0 12px;
        }

        .card__text {
          font-size: 0.9rem;
          line-height: 1.7;
          color: #a8b3c7;
          margin: 0 0 12px;
        }

        .card__text:last-child {
          margin-bottom: 0;
        }

        .card__text--note {
          font-size: 0.85rem;
          color: #6b7a90;
          font-style: italic;
        }

        .card__text strong {
          color: #d0d7e2;
        }

        .card__text em {
          color: #c4785c;
          font-style: normal;
        }

        .card__details {
          display: flex;
          flex-wrap: wrap;
          gap: 24px;
          margin: 16px 0;
          padding: 16px;
          background: rgba(0, 0, 0, 0.2);
          border-radius: 8px;
        }

        .detail {
          display: flex;
          flex-direction: column;
          gap: 4px;
        }

        .detail__label {
          font-size: 0.7rem;
          font-weight: 500;
          color: #6b7a90;
          text-transform: uppercase;
          letter-spacing: 0.08em;
        }

        .detail__value {
          font-size: 0.9rem;
          font-weight: 500;
          color: #e8c4a0;
        }

        .card__list {
          margin: 12px 0;
          padding-left: 20px;
          color: #a8b3c7;
          font-size: 0.9rem;
          line-height: 1.8;
        }

        .card__list li {
          margin-bottom: 4px;
        }

        .card__list strong {
          color: #d0d7e2;
        }

        /* Formula */
        .formula {
          margin: 16px 0;
          padding: 12px 16px;
          background: rgba(0, 0, 0, 0.3);
          border-radius: 6px;
          border-left: 3px solid rgba(196, 120, 92, 0.5);
        }

        .formula code {
          font-family: 'SF Mono', 'Fira Code', monospace;
          font-size: 0.85rem;
          color: #e8c4a0;
        }

        /* Sources */
        .sources {
          display: flex;
          flex-direction: column;
          gap: 12px;
        }

        .source-link {
          display: flex;
          align-items: center;
          gap: 16px;
          padding: 16px 20px;
          background: rgba(22, 25, 32, 0.6);
          border: 1px solid rgba(255, 255, 255, 0.06);
          border-radius: 10px;
          text-decoration: none;
          transition: all 0.2s;
        }

        .source-link:hover {
          background: rgba(22, 25, 32, 0.8);
          border-color: rgba(196, 120, 92, 0.3);
        }

        .source-link__icon {
          font-size: 1.5rem;
        }

        .source-link__text {
          display: flex;
          flex-direction: column;
          flex: 1;
        }

        .source-link__text strong {
          font-size: 0.95rem;
          color: #f0f2f5;
        }

        .source-link__text span {
          font-size: 0.8rem;
          color: #6b7a90;
        }

        .source-link__arrow {
          font-size: 1.2rem;
          color: #6b7a90;
          transition: transform 0.2s;
        }

        .source-link:hover .source-link__arrow {
          transform: translateX(4px);
          color: #c4785c;
        }

        /* Responsive */
        @media (max-width: 768px) {
          .nav {
            padding: 0 16px;
          }

          .nav__links {
            display: none;
          }

          .main {
            padding: 32px 16px 64px;
          }

          .header__title {
            font-size: 2rem;
          }

          .card {
            padding: 20px;
          }

          .card__details {
            flex-direction: column;
            gap: 16px;
          }
        }
      `}</style>
    </div>
  );
}
