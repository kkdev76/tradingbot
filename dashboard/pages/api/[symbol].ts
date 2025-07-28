import { NextApiRequest, NextApiResponse } from 'next'
import fs from 'fs'
import path from 'path'

export default function handler(req: NextApiRequest, res: NextApiResponse) {
  const { symbol } = req.query
  const filePath = path.join(process.cwd(), 'data', `${symbol}_bars.csv`)
  if (!fs.existsSync(filePath)) return res.status(404).json([])

  const content = fs.readFileSync(filePath, 'utf-8')
  const lines = content.trim().split('\n')
  const headers = lines[0].split(',')
  const rows = lines.slice(1).map(line => {
    const parts = line.split(',')
    const entry = {}
    headers.forEach((h, i) => entry[h] = parts[i])
    return entry
  })

  res.status(200).json(rows)
}