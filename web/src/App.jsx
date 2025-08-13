import React, { useState } from 'react'

/**
 * Main application component for the web UI.  Presents a simple
 * question–answering interface backed by the FastAPI service.  A
 * textarea accepts the user's query, the ``Ask`` button triggers the
 * request and the response is rendered below.  A context toggle
 * reveals the top documents that were sent to the Gemini model.
 */
export default function App() {
  const [prompt, setPrompt] = useState('')
  const [answer, setAnswer] = useState('')
  const [context, setContext] = useState('')
  const [runId, setRunId] = useState('')
  const [loading, setLoading] = useState(false)
  const [showContext, setShowContext] = useState(false)

  const handleAsk = async () => {
    if (!prompt.trim()) return
    setLoading(true)
    setAnswer('')
    setContext('')
    setRunId('')
    try {
      const res = await fetch('/api/ask', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt }),
      })
      if (!res.ok) {
        const text = await res.text()
        throw new Error(text)
      }
      const data = await res.json()
      setAnswer(data.answer || '')
      setContext(data.context || '')
      setRunId(data.run_id || data.runId || '')
    } catch (err) {
      setAnswer('Error: ' + err.message)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center p-4">
      <div className="max-w-2xl w-full space-y-4">
        <h1 className="text-3xl font-bold text-center">NYC ACRIS QA</h1>
        <textarea
          className="w-full border border-gray-300 rounded p-2 h-32 resize-none focus:outline-none focus:ring-2 focus:ring-blue-500"
          placeholder="Ask a question..."
          value={prompt}
          onChange={(e) => setPrompt(e.target.value)}
        />
        <button
          className="bg-blue-600 text-white font-medium py-2 px-4 rounded hover:bg-blue-700 disabled:opacity-50"
          onClick={handleAsk}
          disabled={loading}
        >
          {loading ? 'Asking…' : 'Ask'}
        </button>
        {answer && (
          <div className="bg-white border border-gray-200 rounded p-4 shadow">
            <h2 className="font-semibold mb-2">Answer:</h2>
            <p className="whitespace-pre-wrap break-words">{answer}</p>
            {context && (
              <div className="mt-2">
                <button
                  className="text-blue-600 underline"
                  onClick={() => setShowContext(!showContext)}
                >
                  {showContext ? 'Hide context' : 'Show context'}
                </button>
                {showContext && (
                  <pre className="mt-2 p-2 bg-gray-50 border border-gray-200 max-h-64 overflow-auto whitespace-pre-wrap text-sm">
                    {context}
                  </pre>
                )}
              </div>
            )}
            {runId && (
              <p className="text-xs text-gray-500 mt-2">Run ID: {runId}</p>
            )}
          </div>
        )}
      </div>
    </div>
  )
}