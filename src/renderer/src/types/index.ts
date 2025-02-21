export interface ExistingPage {
    id: string;
    title: string;
    lastEditedTime: string;
}

export interface UploadedPage {
    pageId: string;
    bookTitle: string;
}

export interface ExportProgress {
    currentBook: string;
    currentStep: string;
    completed: number;
} 