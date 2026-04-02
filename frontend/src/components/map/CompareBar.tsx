"use client";

import { useRouter } from "next/navigation";
import type { MunicipalityData } from "./MunicipalityDrawer";

interface CompareBarProps {
  municipalities: MunicipalityData[];
  onRemove: (id: string) => void;
  onClear: () => void;
}

export function CompareBar({ municipalities, onRemove, onClear }: CompareBarProps) {
  const router = useRouter();

  if (municipalities.length === 0) return null;

  const handleCompare = () => {
    const ids = municipalities.map((m) => m.municipalityId).join(",");
    router.push(`/compare?ids=${ids}`);
  };

  return (
    <div className="compare-bar">
      <div className="compare-bar__content">
        <div className="compare-bar__header">
          <span className="compare-bar__title">
            Compare ({municipalities.length}/5)
          </span>
          <button className="compare-bar__clear" onClick={onClear}>
            Clear all
          </button>
        </div>

        <div className="compare-bar__items">
          {municipalities.map((m) => (
            <div key={m.municipalityId} className="compare-bar__item">
              <span className="compare-bar__item-name">{m.name}</span>
              <button
                className="compare-bar__item-remove"
                onClick={() => onRemove(m.municipalityId)}
                aria-label={`Remove ${m.name}`}
              >
                <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
                  <path
                    d="M3 3L11 11M11 3L3 11"
                    stroke="currentColor"
                    strokeWidth="1.5"
                    strokeLinecap="round"
                  />
                </svg>
              </button>
            </div>
          ))}
        </div>

        <button
          className="compare-bar__button"
          onClick={handleCompare}
          disabled={municipalities.length < 2}
        >
          {municipalities.length < 2
            ? "Select at least 2 municipalities"
            : "Compare Now"}
        </button>
      </div>

      <style jsx>{`
        .compare-bar {
          position: fixed;
          bottom: 0;
          left: 0;
          right: 0;
          z-index: 1000;
          background: linear-gradient(
            to top,
            rgba(26, 26, 24, 0.98) 0%,
            rgba(26, 26, 24, 0.95) 100%
          );
          border-top: 1px solid rgba(196, 120, 92, 0.3);
          backdrop-filter: blur(12px);
          padding: 16px 24px;
          animation: slideUp 0.3s ease-out;
        }

        @keyframes slideUp {
          from {
            transform: translateY(100%);
            opacity: 0;
          }
          to {
            transform: translateY(0);
            opacity: 1;
          }
        }

        .compare-bar__content {
          max-width: 1200px;
          margin: 0 auto;
          display: flex;
          flex-direction: column;
          gap: 12px;
        }

        .compare-bar__header {
          display: flex;
          justify-content: space-between;
          align-items: center;
        }

        .compare-bar__title {
          font-size: 14px;
          font-weight: 600;
          color: #c4785c;
        }

        .compare-bar__clear {
          background: none;
          border: none;
          color: rgba(255, 255, 255, 0.5);
          font-size: 12px;
          cursor: pointer;
          padding: 4px 8px;
          transition: color 0.2s;
        }

        .compare-bar__clear:hover {
          color: rgba(255, 255, 255, 0.8);
        }

        .compare-bar__items {
          display: flex;
          flex-wrap: wrap;
          gap: 8px;
        }

        .compare-bar__item {
          display: flex;
          align-items: center;
          gap: 6px;
          background: rgba(255, 255, 255, 0.08);
          border: 1px solid rgba(255, 255, 255, 0.1);
          border-radius: 20px;
          padding: 6px 10px 6px 14px;
        }

        .compare-bar__item-name {
          font-size: 13px;
          color: rgba(255, 255, 255, 0.9);
        }

        .compare-bar__item-remove {
          background: none;
          border: none;
          color: rgba(255, 255, 255, 0.4);
          cursor: pointer;
          padding: 2px;
          display: flex;
          align-items: center;
          justify-content: center;
          transition: color 0.2s;
        }

        .compare-bar__item-remove:hover {
          color: #e57373;
        }

        .compare-bar__button {
          background: linear-gradient(135deg, #c4785c 0%, #a85d3f 100%);
          border: none;
          border-radius: 8px;
          color: #fff;
          font-size: 14px;
          font-weight: 600;
          padding: 12px 24px;
          cursor: pointer;
          transition: all 0.2s;
          align-self: flex-end;
        }

        .compare-bar__button:hover:not(:disabled) {
          background: linear-gradient(135deg, #d4886c 0%, #b86d4f 100%);
          transform: translateY(-1px);
        }

        .compare-bar__button:disabled {
          opacity: 0.5;
          cursor: not-allowed;
        }

        @media (min-width: 640px) {
          .compare-bar__content {
            flex-direction: row;
            align-items: center;
            justify-content: space-between;
          }

          .compare-bar__header {
            flex-direction: column;
            align-items: flex-start;
            gap: 4px;
          }

          .compare-bar__items {
            flex: 1;
            margin: 0 16px;
          }

          .compare-bar__button {
            align-self: center;
          }
        }
      `}</style>
    </div>
  );
}
