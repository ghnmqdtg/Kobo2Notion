import { Button } from '@/components/ui/button';
import { Progress } from '@/components/ui/progress';

interface FooterProps {
    selectedCount: number;
    totalSelected: number;
    isExporting: boolean;
    currentBook?: string;
    currentStep?: string;
    onExport: () => void;
}

export function Footer({
    selectedCount,
    totalSelected,
    isExporting,
    currentBook,
    currentStep,
    onExport
}: FooterProps) {
    return (
        <div id="footer" className="fixed bottom-0 bg-background backdrop-blur-none border-t">
            <div className="container mx-auto px-4 h-16 flex items-center justify-between">
                <div className="flex flex-col gap-2 flex-1 mr-4">
                    {isExporting ? (
                        <>
                            <div className="flex justify-between text-sm text-muted-foreground">
                                <span>{currentBook}</span>
                                <span>{currentStep}</span>
                            </div>
                            <Progress value={(selectedCount / totalSelected) * 100} />
                        </>
                    ) : (
                        <div className="text-md font-bold">
                            {selectedCount > 0
                                ? `${selectedCount} book${selectedCount > 1 ? 's' : ''} selected`
                                : 'Select books to export'
                            }
                        </div>
                    )}
                </div>
                <Button className="text-md font-bold" onClick={onExport} disabled={selectedCount === 0 || isExporting}>
                    {isExporting ? 'Exporting...' : 'Export to Notion'}
                </Button>
            </div>
        </div>
    );
} 