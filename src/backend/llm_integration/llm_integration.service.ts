import {
  GoogleGenerativeAI,
  GenerateContentRequest,
  GenerateContentResult,
} from "@google/generative-ai";
import { Bookmark } from "../models";
import { env } from "../../config/env.config";

export class GeminiService {
  private genAI: GoogleGenerativeAI;
  private model: any;
  // private summarizeEnabled: boolean;
  // private summarizeLanguage: string;

  constructor() {
    this.genAI = new GoogleGenerativeAI(env.GEMINI_API_KEY);
    this.model = this.genAI.getGenerativeModel({ model: env.GEMINI_MODEL });
    // We don't set these variables in the constructor because we want to pass them in the function call
    // this.summarizeEnabled = env.SUMMARIZE_ENABLED;
    // this.summarizeLanguage = env.SUMMARIZE_LANGUAGE;
  }

  async summarizeBookmarks(
    bookTitle: string,
    bookmarks: Bookmark[],
    summarizeLanguage: string,
  ): Promise<string> {
    const content = bookmarks.map((b) => b.highlight).join("\n");
    const prompt = this.generatePrompt(bookTitle, content, summarizeLanguage);
    const request: GenerateContentRequest = {
      contents: [{ role: "user", parts: [{ text: prompt }] }],
    };
    const result: GenerateContentResult =
      await this.model.generateContent(request);
    const response = await result.response;
    const text = response.text();
    return text;
  }

  private generatePrompt(
    bookTitle: string,
    content: string,
    summarizeLanguage: string,
  ): string {
    if (summarizeLanguage === "en") {
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
        `;
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
        `;
    }
  }
}
