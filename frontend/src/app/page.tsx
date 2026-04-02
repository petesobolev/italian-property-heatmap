import Link from "next/link";

export default function Home() {
  return (
    <div className="min-h-screen bg-zinc-50 font-sans text-zinc-950">
      <main className="mx-auto flex w-full max-w-5xl flex-col gap-10 px-6 py-14">
        <header className="flex flex-col gap-3">
          <p className="text-sm font-medium tracking-wide text-zinc-500">
            Italy municipality heat map
          </p>
          <h1 className="text-balance text-4xl font-semibold tracking-tight">
            Forecasting euro/m², appreciation, and rental ROI by comune.
          </h1>
          <p className="max-w-2xl text-pretty text-lg leading-8 text-zinc-600">
            Analytics-first map product built around municipality-level scoring: current
            valuation, 12-month appreciation forecast, 12-month long-term rental yield
            forecast, and a confidence score.
          </p>
        </header>

        <section className="grid gap-4 md:grid-cols-3">
          {[
            {
              title: "Map",
              desc: "Choropleth by metric, segment, horizon, and filters.",
              href: "/map",
            },
            {
              title: "Rankings",
              desc: "Sortable opportunity tables by strategy and risk.",
              href: "/rankings",
            },
            {
              title: "Methodology",
              desc: "Sources, modeling, confidence, and limitations.",
              href: "/methodology",
            },
          ].map((card) => (
            <Link
              key={card.href}
              href={card.href}
              className="rounded-2xl border border-zinc-200 bg-white p-5 shadow-sm transition hover:-translate-y-0.5 hover:shadow-md"
            >
              <div className="text-base font-semibold">{card.title}</div>
              <div className="mt-2 text-sm leading-6 text-zinc-600">{card.desc}</div>
              <div className="mt-4 text-sm font-medium text-zinc-900">
                Open <span aria-hidden="true">→</span>
              </div>
            </Link>
          ))}
        </section>

        <section className="rounded-2xl border border-zinc-200 bg-white p-5 text-sm leading-6 text-zinc-600">
          Tip: start with the <span className="font-medium text-zinc-900">Map</span>{" "}
          page. The UI will call Next.js API routes (route handlers) that later read from
          Supabase/PostGIS views of the latest published forecast snapshot.
        </section>
      </main>
    </div>
  );
}
