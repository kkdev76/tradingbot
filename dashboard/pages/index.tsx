
import { useEffect, useState } from "react"
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs"
import { Card, CardContent } from "@/components/ui/card"

export default function Home() {
  const [log, setLog] = useState("Loading...")

  useEffect(() => {
    const fetchLog = async () => {
      try {
        const res = await fetch("http://localhost:8000/trading_log.txt")
        const text = await res.text()
        setLog(text)
      } catch (err) {
        setLog("⚠️ Could not load log file")
      }
    }
    fetchLog()
    const interval = setInterval(fetchLog, 2000)
    return () => clearInterval(interval)
  }, [])

  return (
    <main className="p-4">
      <Tabs defaultValue="log">
        <TabsList>
          <TabsTrigger value="log">Trade Log</TabsTrigger>
          <TabsTrigger value="summary">Summary</TabsTrigger>
        </TabsList>
        <TabsContent value="log">
          <Card className="mt-4">
            <CardContent>
              <pre className="whitespace-pre-wrap text-sm overflow-y-auto max-h-[70vh]">
                {log}
              </pre>
            </CardContent>
          </Card>
        </TabsContent>
        <TabsContent value="summary">
          <Card className="mt-4">
            <CardContent>
              Coming soon: charts & analytics.
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </main>
  )
}
