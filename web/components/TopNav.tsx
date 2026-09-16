"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const LINKS = [
  { href: "/", label: "Overview" },
  { href: "/benchmarks", label: "Benchmarks" },
];

export function TopNav() {
  const pathname = usePathname();

  return (
    <header className="flex h-14 items-center gap-6 border-b border-border px-6">
      <div className="flex items-center gap-2">
        <div className="flex size-6 items-center justify-center rounded-md bg-ink-primary">
          <span className="text-[13px] font-bold text-page">R</span>
        </div>
        <span className="text-[14px] font-medium text-ink-primary">Relay</span>
      </div>
      <nav className="flex items-center gap-4">
        {LINKS.map((link) => {
          const active = pathname === link.href;
          return (
            <Link
              key={link.href}
              href={link.href}
              className={`text-[13px] ${active ? "text-ink-primary" : "text-ink-secondary hover:text-ink-primary"}`}
            >
              {link.label}
            </Link>
          );
        })}
      </nav>
    </header>
  );
}
