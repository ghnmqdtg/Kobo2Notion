// This file serves as a central location to define the data structures (or data models) that our application works with.

// Kobo Models
export type BookSource = 'kobo-store' | 'instapaper' | 'external';

export interface Book {
  bookTitle: string;
  subtitle: string | null;
  author: string;
  publisher: string;
  isbn: string;
  series: string | null;
  seriesNumber: number | null;
  readPercent: number;
  imageId: string | null;
  bookmarkCount?: number; // Optional for backward compatibility
  source: BookSource;
}

export interface Bookmark {
  volumeId: string;
  highlight: string;
  annotation: string | null;
  createdOn: string;
  type: string;
}

// Notion Block Types

type ParagraphBlock = {
  type: "paragraph";
  paragraph: {
    rich_text: RichTextItem[];
    children?: Block[];
  };
};

type Heading1Block = {
  type: "heading_1";
  heading_1: {
    rich_text: RichTextItem[];
    children?: Block[];
  };
};

type Heading2Block = {
  type: "heading_2";
  heading_2: {
    rich_text: RichTextItem[];
    children?: Block[];
  };
};

type Heading3Block = {
  type: "heading_3";
  heading_3: {
    rich_text: RichTextItem[];
    children?: Block[];
  };
};

type BulletedListItemBlock = {
  type: "bulleted_list_item";
  bulleted_list_item: {
    rich_text: RichTextItem[];
    children?: Block[];
  };
};

type NumberedListItemBlock = {
  type: "numbered_list_item";
  numbered_list_item: {
    rich_text: RichTextItem[];
    children?: Block[];
  };
};

type QuoteBlock = {
  type: "quote";
  quote: {
    rich_text: RichTextItem[];
    children?: Block[];
  };
};

export type RichTextItem = {
  type: "text";
  text: {
    content: string;
    link?: string | null;
  };
  annotations?: {
    bold?: boolean;
    italic?: boolean;
    strikethrough?: boolean;
    underline?: boolean;
    code?: boolean;
    color?: string;
  };
  plain_text?: string;
  href?: string | null;
};

// Add other block types as needed...

export type Block =
  | ParagraphBlock
  | Heading1Block
  | Heading2Block
  | Heading3Block
  | BulletedListItemBlock
  | NumberedListItemBlock
  | QuoteBlock;

export type NotionBlock = Block & { object: "block"; };
