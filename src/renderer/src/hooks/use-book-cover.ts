import { useState, useEffect } from 'react'
import { useNetworkState } from '@uidotdev/usehooks'

// In-memory cache for cover image data URIs
const imageCache = new Map<string, string>()

interface UseBookCoverReturn {
  coverDataUrl: string | null
  isLoading: boolean
  error: string | null
}

export function useBookCover(imageId: string | null): UseBookCoverReturn {
  const [coverDataUrl, setCoverDataUrl] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const networkState = useNetworkState()

  useEffect(() => {
    if (!imageId) {
      setCoverDataUrl(null)
      setIsLoading(false)
      return
    }

    let isCancelled = false

    const loadCover = async (): Promise<void> => {
      // Return cached image if available
      if (imageCache.has(imageId)) {
        setCoverDataUrl(imageCache.get(imageId)!)
        return
      }

      // Skip fetching if offline
      if (!networkState.online) {
        setError('Offline')
        return
      }

      setIsLoading(true)
      setError(null)

      try {
        const url = await window.api.fetchBookCover(imageId)
        const response = await fetch(url)

        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`)
        }

        const blob = await response.blob()

        // Convert blob to Base64 data URI
        const reader = new FileReader()
        reader.onloadend = (): void => {
          if (!isCancelled) {
            const dataUrl = reader.result as string
            imageCache.set(imageId, dataUrl)
            setCoverDataUrl(dataUrl)
            setIsLoading(false)
          }
        }
        reader.onerror = (): void => {
          if (!isCancelled) {
            setError('Failed to read blob data.')
            setIsLoading(false)
          }
        }
        reader.readAsDataURL(blob)
      } catch (err) {
        console.error('Error loading book cover:', err)
        if (!isCancelled) {
          setError(err instanceof Error ? err.message : 'An unknown error occurred.')
          setIsLoading(false)
        }
      }
    }

    loadCover()

    return (): void => {
      isCancelled = true
    }
  }, [imageId, networkState.online])

  return { coverDataUrl, isLoading, error }
}
