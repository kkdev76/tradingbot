
import * as React from "react"

export function Card({ children }: { children: React.ReactNode }) {
  return <div className="rounded-xl border p-4 shadow">{children}</div>
}

export function CardContent({ children }: { children: React.ReactNode }) {
  return <div className="mt-2">{children}</div>
}
