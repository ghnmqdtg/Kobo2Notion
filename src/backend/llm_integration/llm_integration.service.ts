import { generateText, LanguageModel } from 'ai'
import { createGoogleGenerativeAI } from '@ai-sdk/google'
import { createOpenAI } from '@ai-sdk/openai'
import { createAnthropic } from '@ai-sdk/anthropic'
import { Bookmark } from '../models'
import { env } from '../../config/env.config'

interface GoogleModel {
  name: string
  supportedGenerationMethods?: string[]
}

interface ModelEntry {
  id: string
}

const fallbackModels: Record<string, string[]> = {
  google: ['gemini-2.5-flash', 'gemini-2.5-pro'],
  openai: ['gpt-4o', 'gpt-4o-mini', 'gpt-4.1', 'gpt-4.1-mini'],
  anthropic: ['claude-sonnet-4-5-20250514', 'claude-haiku-4-5-20251001']
}

export async function fetchAvailableModels(provider: string, apiKey: string): Promise<string[]> {
  try {
    switch (provider) {
      case 'google':
        return await fetchGoogleModels(apiKey)
      case 'openai':
        return await fetchOpenAIModels(apiKey)
      case 'anthropic':
        return await fetchAnthropicModels(apiKey)
      default:
        return []
    }
  } catch (error) {
    console.warn(`Failed to fetch models for ${provider}, using fallbacks:`, error)
    return fallbackModels[provider] || []
  }
}

async function fetchGoogleModels(apiKey: string): Promise<string[]> {
  const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/models?key=${apiKey}`)
  if (!res.ok) throw new Error(`Google API error: ${res.status}`)
  const data = await res.json()
  return (data.models || [])
    .filter((m: GoogleModel) => m.supportedGenerationMethods?.includes('generateContent'))
    .map((m: GoogleModel) => m.name.replace('models/', ''))
    .sort()
}

async function fetchOpenAIModels(apiKey: string): Promise<string[]> {
  const res = await fetch('https://api.openai.com/v1/models', {
    headers: { Authorization: `Bearer ${apiKey}` }
  })
  if (!res.ok) throw new Error(`OpenAI API error: ${res.status}`)
  const data = await res.json()
  return (data.data || [])
    .map((m: ModelEntry) => m.id)
    .filter((id: string) => /^(gpt-|o[0-9])/.test(id))
    .sort()
}

async function fetchAnthropicModels(apiKey: string): Promise<string[]> {
  const res = await fetch('https://api.anthropic.com/v1/models', {
    headers: {
      'x-api-key': apiKey,
      'anthropic-version': '2023-06-01'
    }
  })
  if (!res.ok) throw new Error(`Anthropic API error: ${res.status}`)
  const data = await res.json()
  return (data.data || []).map((m: ModelEntry) => m.id).sort()
}

export class LLMService {
  private getModel(): LanguageModel {
    const { LLM_PROVIDER, LLM_API_KEY, LLM_MODEL } = env
    switch (LLM_PROVIDER) {
      case 'google':
        return createGoogleGenerativeAI({ apiKey: LLM_API_KEY })(LLM_MODEL)
      case 'openai':
        return createOpenAI({ apiKey: LLM_API_KEY })(LLM_MODEL)
      case 'anthropic':
        return createAnthropic({ apiKey: LLM_API_KEY })(LLM_MODEL)
      default:
        throw new Error(`Unsupported LLM provider: ${LLM_PROVIDER}`)
    }
  }

  async summarizeBookmarks(
    bookTitle: string,
    bookmarks: Bookmark[],
    summarizeLanguage: string
  ): Promise<string> {
    const content = bookmarks.map((b) => b.highlight).join('\n')
    const prompt = this.generatePrompt(bookTitle, content, summarizeLanguage)
    const { text } = await generateText({
      model: this.getModel(),
      prompt
    })
    return text
  }

  private generatePrompt(bookTitle: string, content: string, summarizeLanguage: string): string {
    if (summarizeLanguage === 'en') {
      return `
        The following is a list of highlights from a book: ${bookTitle}.
        \`\`\`
        ${content}
        \`\`\`

        Please summarize the highlights into a concise and coherent summary using markdown format. Here are some guidelines:
        1. The highlights are ordered, but don't have a specific chapter or section, so please group them into sections.
        2. Please use bold text to highlight the most important words or sentences.
        3. It's okay to have numbers in the heading, such as "# 1. Section Title" or "# 二、段落標題"
        4. If there are duplicate highlights, please remove them to keep the summary concise.
        5. Please add abstract at the beginning and conclusion at the end, both with heading.
        `
    } else {
      return `
        以下是從《${bookTitle}》節錄的重點：
        \`\`\`
        ${content}
        \`\`\`
        請幫我以 markdown 格式統整、濃縮筆記，謝謝。以下為注意事項：
        1. 請以繁體中文回答。
        2. 這些重點的順序是連續的，但可能分散於不同章節，請依內容自行分類統整。謝謝。
        3. 直接回答重點，不要有任何額外的說明。
        4. 若有段落，其 heading 標籤可以同時附帶標號以更加醒目，例如：「# 一、段落標題」，其內容則以 numbered list 或 bullet point 表示。
        5. 冒號和括號以全形「：」和「（）」表示。
        6. 中、英文及數字間以半形空格隔開。
        7. 若重點有所重複，可以刪減以保持簡潔。
        8. 請於最開頭加上摘要，並於最後加上總結，兩段落皆使用 heading。
        `
    }
  }
}
