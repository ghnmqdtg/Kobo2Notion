import {
    AlertDialog,
    AlertDialogContent,
    AlertDialogDescription,
    AlertDialogFooter,
    AlertDialogHeader,
    AlertDialogTitle,
    AlertDialogCancel,
    AlertDialogAction,
} from "@/components/ui/alert-dialog";
import { Checkbox } from "@/components/ui/checkbox";
import { ScrollArea } from "@/components/ui/scroll-area";
import { useState, useEffect } from "react";

interface ExistingPage {
    id: string;
    title: string;
    lastEditedTime: string;
}

interface ConfirmOverwriteDialogProps {
    existingPages: ExistingPage[];
    open: boolean;
    onOpenChange: (open: boolean) => void;
    onConfirm: (selectedPages: string[]) => void;
    onCancel: () => void;
}

export function ConfirmOverwriteDialog({
    existingPages,
    open,
    onOpenChange,
    onConfirm,
    onCancel,
}: ConfirmOverwriteDialogProps) {
    const [selectedPages, setSelectedPages] = useState<Set<string>>(
        new Set(existingPages.map(page => page.id))
    );

    useEffect(() => {
        if (open) {
            setSelectedPages(new Set(existingPages.map(page => page.id)));
        }
    }, [open, existingPages]);

    const handleCheckboxChange = (pageId: string, checked: boolean) => {
        setSelectedPages(prev => {
            const newSet = new Set(prev);
            if (checked) {
                newSet.add(pageId);
            } else {
                newSet.delete(pageId);
            }
            return newSet;
        });
    };

    const handleConfirm = () => {
        onConfirm(Array.from(selectedPages));
        setSelectedPages(new Set());
    };

    const handleCancel = () => {
        setSelectedPages(new Set());
        onCancel();
    };

    return (
        <AlertDialog
            open={open}
            onOpenChange={(open) => {
                if (!open) {
                    handleCancel();
                }
                onOpenChange(open);
            }}
        >
            <AlertDialogContent>
                <AlertDialogDescription></AlertDialogDescription>
                <AlertDialogHeader>
                    <AlertDialogTitle>Overwrite Existing Pages?</AlertDialogTitle>
                    <div className="text-sm text-muted-foreground">
                        The following books already exist in your Notion database.
                        Select the ones you want to overwrite:
                    </div>
                    <ScrollArea className="max-h-[300px] rounded-md">
                        <div className="mt-4 space-y-3">
                            {existingPages.map((page) => (
                                <div key={page.id} className="flex items-start space-x-3 mt-2 p-2 rounded-md hover:bg-accent/50 cursor-pointer">
                                    <Checkbox
                                        id={page.id}
                                        checked={selectedPages.has(page.id)}
                                        onCheckedChange={(checked) =>
                                            handleCheckboxChange(page.id, checked as boolean)
                                        }
                                    />
                                    <label htmlFor={page.id} className="text-sm leading-none w-full cursor-pointer">
                                        <div>{page.title}</div>
                                        <div className="text-xs text-muted-foreground mt-1">
                                            Last edited: {new Date(page.lastEditedTime).toLocaleString('zh-TW', {
                                                year: 'numeric',
                                                month: '2-digit',
                                                day: '2-digit',
                                                hour: '2-digit',
                                                minute: '2-digit',
                                                hour12: false
                                            })}
                                        </div>
                                    </label>
                                </div>
                            ))}
                        </div>
                    </ScrollArea>
                </AlertDialogHeader>
                <AlertDialogFooter>
                    <AlertDialogCancel onClick={handleCancel}>
                        Cancel
                    </AlertDialogCancel>
                    <AlertDialogAction
                        onClick={handleConfirm}
                        disabled={selectedPages.size === 0}
                    >
                        Overwrite Selected
                    </AlertDialogAction>
                </AlertDialogFooter>
            </AlertDialogContent>
        </AlertDialog>
    );
} 