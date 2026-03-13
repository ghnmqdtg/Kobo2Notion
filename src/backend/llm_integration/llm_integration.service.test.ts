import { describe, expect, beforeAll, it } from "@jest/globals";
import { LLMService } from "./llm_integration.service";
import { Bookmark } from "../models";

// **Important:**
// 1. Set your LLM_PROVIDER, LLM_API_KEY, and LLM_MODEL in your environment variables (e.g., in a .env file).
// 2. Be mindful of the API usage limits and costs when running tests that make real API calls.
// 3. Consider using a separate test API key with restricted permissions or usage limits.

describe("LLMService (Integration Tests)", () => {
  let service: LLMService;

  beforeAll(() => {
    // Initialize the service before running any tests
    service = new LLMService();
  });

  describe("summarizeBookmarks", () => {
    const mockBookmarks: Bookmark[] = [
      {
        volumeId: "some-volume-id",
        highlight: "This is a key concept from the book.",
        annotation: null,
        createdOn: "2023-10-26",
        type: "highlight",
      },
      {
        volumeId: "some-volume-id",
        highlight: "Another important point to remember.",
        annotation: null,
        createdOn: "2023-10-27",
        type: "highlight",
      },
      {
        volumeId: "some-volume-id",
        highlight: "A supporting detail for the key concept.",
        annotation: null,
        createdOn: "2023-10-28",
        type: "highlight",
      },
    ];

    it("should generate a valid summary in English", async () => {
      const bookTitle = "Test Book";
      const summaryLanguage = "en";

      const summary = await service.summarizeBookmarks(
        bookTitle,
        mockBookmarks,
        summaryLanguage,
      );

      console.log("English Summary:", summary); // Log the summary for inspection

      expect(summary).toBeTruthy(); // Check that a summary was generated
      expect(summary.length).toBeGreaterThan(0); // Check that the summary is not empty

      // Add more specific assertions based on your expectations of the summary format:
      expect(summary).toContain("**"); // Check for bold text (if your prompt uses bolding)

      // You might need to add more specific assertions to validate the structure and content of the summary
      // based on the requirements in your prompt.
    });

    it("should generate a valid summary in Traditional Chinese", async () => {
      const bookTitle = "測試書籍";
      const summaryLanguage = "zh";

      const summary = await service.summarizeBookmarks(
        bookTitle,
        mockBookmarks,
        summaryLanguage,
      );

      console.log("Traditional Chinese Summary:", summary); // Log the summary for inspection

      expect(summary).toBeTruthy();
      expect(summary.length).toBeGreaterThan(0);

      // Add assertions specific to the Traditional Chinese prompt and expected output:
      expect(summary).toContain("。"); // Check for full-width period (common in Chinese)

      // Add more specific assertions to validate the structure and content of the summary.
    });
  });
});
