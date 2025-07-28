
import * as React from "react"

export function Tabs({ children }: { children: React.ReactNode }) {
  return <div className="tabs">{children}</div>
}

export function TabsList({ children }: { children: React.ReactNode }) {
  return <div className="tabs-list">{children}</div>
}

export function TabsTrigger({ children }: { children: React.ReactNode }) {
  return <button className="tabs-trigger">{children}</button>
}

export function TabsContent({ children }: { children: React.ReactNode }) {
  return <div className="tabs-content">{children}</div>
}
