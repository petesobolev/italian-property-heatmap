"use client";

import { useState, useEffect, useCallback, useRef } from "react";
import { createPortal } from "react-dom";

interface SearchResult {
  id: string;
  name: string;
  type: "region" | "province" | "municipality";
  parentName?: string;
  bounds: [[number, number], [number, number]];
}

interface CommandPaletteProps {
  isOpen: boolean;
  onClose: () => void;
  onSelectLocation: (bounds: [[number, number], [number, number]]) => void;
}

const TYPE_ICONS: Record<string, string> = {
  region: "🏛",
  province: "📍",
  municipality: "🏘",
};

const TYPE_LABELS: Record<string, string> = {
  region: "Region",
  province: "Province",
  municipality: "Municipality",
};

export function CommandPalette({ isOpen, onClose, onSelectLocation }: CommandPaletteProps) {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<SearchResult[]>([]);
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [isLoading, setIsLoading] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const resultsRef = useRef<HTMLDivElement>(null);

  // Focus input when opened
  useEffect(() => {
    if (isOpen) {
      setQuery("");
      setResults([]);
      setSelectedIndex(0);
      setTimeout(() => inputRef.current?.focus(), 10);
    }
  }, [isOpen]);

  // Search as user types
  useEffect(() => {
    if (!query || query.length < 2) {
      setResults([]);
      return;
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(async () => {
      setIsLoading(true);
      try {
        const response = await fetch(
          `/api/locations/search?q=${encodeURIComponent(query)}`,
          { signal: controller.signal }
        );
        if (response.ok) {
          const data = await response.json();
          setResults(data.results);
          setSelectedIndex(0);
        }
      } catch (error) {
        if ((error as Error).name !== "AbortError") {
          console.error("Search error:", error);
        }
      } finally {
        setIsLoading(false);
      }
    }, 150); // Debounce

    return () => {
      clearTimeout(timeoutId);
      controller.abort();
    };
  }, [query]);

  // Scroll selected item into view
  useEffect(() => {
    if (resultsRef.current && results.length > 0) {
      const selectedElement = resultsRef.current.children[selectedIndex] as HTMLElement;
      selectedElement?.scrollIntoView({ block: "nearest" });
    }
  }, [selectedIndex, results.length]);

  // Handle keyboard navigation
  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      switch (e.key) {
        case "ArrowDown":
          e.preventDefault();
          setSelectedIndex((i) => Math.min(i + 1, results.length - 1));
          break;
        case "ArrowUp":
          e.preventDefault();
          setSelectedIndex((i) => Math.max(i - 1, 0));
          break;
        case "Enter":
          e.preventDefault();
          if (results[selectedIndex]) {
            onSelectLocation(results[selectedIndex].bounds);
            onClose();
          }
          break;
        case "Escape":
          e.preventDefault();
          onClose();
          break;
      }
    },
    [results, selectedIndex, onSelectLocation, onClose]
  );

  const handleResultClick = (result: SearchResult) => {
    onSelectLocation(result.bounds);
    onClose();
  };

  if (!isOpen) return null;

  return createPortal(
    <div className="command-palette-overlay" onClick={onClose}>
      <div
        className="command-palette"
        onClick={(e) => e.stopPropagation()}
        onKeyDown={handleKeyDown}
      >
        <div className="command-palette__input-wrapper">
          <svg
            className="command-palette__search-icon"
            width="20"
            height="20"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
          >
            <circle cx="11" cy="11" r="8" />
            <path d="m21 21-4.35-4.35" />
          </svg>
          <input
            ref={inputRef}
            type="text"
            className="command-palette__input"
            placeholder="Search regions, provinces, or municipalities..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
          {isLoading && <div className="command-palette__spinner" />}
          <kbd className="command-palette__kbd">ESC</kbd>
        </div>

        {results.length > 0 && (
          <div className="command-palette__results" ref={resultsRef}>
            {results.map((result, index) => (
              <button
                key={result.id}
                className={`command-palette__result ${index === selectedIndex ? "command-palette__result--selected" : ""}`}
                onClick={() => handleResultClick(result)}
                onMouseEnter={() => setSelectedIndex(index)}
              >
                <span className="command-palette__result-icon">
                  {TYPE_ICONS[result.type]}
                </span>
                <div className="command-palette__result-text">
                  <span className="command-palette__result-name">{result.name}</span>
                  {result.parentName && (
                    <span className="command-palette__result-parent">
                      {result.parentName}
                    </span>
                  )}
                </div>
                <span className="command-palette__result-type">
                  {TYPE_LABELS[result.type]}
                </span>
              </button>
            ))}
          </div>
        )}

        {query.length >= 2 && results.length === 0 && !isLoading && (
          <div className="command-palette__empty">
            No locations found for &ldquo;{query}&rdquo;
          </div>
        )}

        {query.length < 2 && (
          <div className="command-palette__hint">
            <p>Type at least 2 characters to search</p>
            <div className="command-palette__hint-examples">
              <span>Try: <em>Roma</em>, <em>Toscana</em>, <em>Milano</em></span>
            </div>
          </div>
        )}
      </div>

      <style jsx global>{`
        .command-palette-overlay {
          position: fixed;
          inset: 0;
          z-index: 9999;
          background: rgba(0, 0, 0, 0.5);
          backdrop-filter: blur(4px);
          display: flex;
          align-items: flex-start;
          justify-content: center;
          padding-top: 15vh;
        }

        .command-palette {
          width: 100%;
          max-width: 560px;
          background: linear-gradient(
            165deg,
            rgba(22, 25, 32, 0.98) 0%,
            rgba(13, 15, 18, 0.99) 100%
          );
          border: 1px solid rgba(255, 255, 255, 0.1);
          border-radius: 16px;
          box-shadow:
            0 25px 50px -12px rgba(0, 0, 0, 0.5),
            0 0 0 1px rgba(255, 255, 255, 0.05);
          overflow: hidden;
        }

        .command-palette__input-wrapper {
          display: flex;
          align-items: center;
          gap: 12px;
          padding: 16px 20px;
          border-bottom: 1px solid rgba(255, 255, 255, 0.08);
        }

        .command-palette__search-icon {
          color: #6b7a90;
          flex-shrink: 0;
        }

        .command-palette__input {
          flex: 1;
          background: transparent;
          border: none;
          outline: none;
          font-size: 1.1rem;
          color: #f0f2f5;
          font-family: "DM Sans", -apple-system, sans-serif;
        }

        .command-palette__input::placeholder {
          color: #4a5568;
        }

        .command-palette__spinner {
          width: 18px;
          height: 18px;
          border: 2px solid rgba(196, 120, 92, 0.3);
          border-top-color: #c4785c;
          border-radius: 50%;
          animation: spin 0.8s linear infinite;
        }

        @keyframes spin {
          to {
            transform: rotate(360deg);
          }
        }

        .command-palette__kbd {
          padding: 4px 8px;
          background: rgba(255, 255, 255, 0.06);
          border: 1px solid rgba(255, 255, 255, 0.1);
          border-radius: 6px;
          font-size: 0.7rem;
          font-family: monospace;
          color: #6b7a90;
        }

        .command-palette__results {
          max-height: 400px;
          overflow-y: auto;
          padding: 8px;
        }

        .command-palette__result {
          display: flex;
          align-items: center;
          gap: 12px;
          width: 100%;
          padding: 12px 16px;
          background: transparent;
          border: none;
          border-radius: 10px;
          cursor: pointer;
          text-align: left;
          transition: background-color 0.1s;
        }

        .command-palette__result:hover,
        .command-palette__result--selected {
          background: rgba(196, 120, 92, 0.15);
        }

        .command-palette__result-icon {
          font-size: 1.25rem;
          width: 32px;
          height: 32px;
          display: flex;
          align-items: center;
          justify-content: center;
          background: rgba(255, 255, 255, 0.05);
          border-radius: 8px;
        }

        .command-palette__result-text {
          flex: 1;
          min-width: 0;
        }

        .command-palette__result-name {
          display: block;
          font-size: 0.95rem;
          font-weight: 500;
          color: #f0f2f5;
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
        }

        .command-palette__result-parent {
          display: block;
          font-size: 0.8rem;
          color: #6b7a90;
          margin-top: 2px;
        }

        .command-palette__result-type {
          font-size: 0.7rem;
          text-transform: uppercase;
          letter-spacing: 0.05em;
          color: #4a5568;
          padding: 4px 8px;
          background: rgba(255, 255, 255, 0.04);
          border-radius: 4px;
        }

        .command-palette__empty,
        .command-palette__hint {
          padding: 24px;
          text-align: center;
          color: #6b7a90;
        }

        .command-palette__hint-examples {
          margin-top: 12px;
          font-size: 0.85rem;
        }

        .command-palette__hint-examples em {
          color: #c4785c;
          font-style: normal;
        }
      `}</style>
    </div>,
    document.body
  );
}
