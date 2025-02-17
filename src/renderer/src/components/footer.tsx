import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import { useNetworkState } from "@uidotdev/usehooks";
import { AlertCircle, Loader2, XCircle } from "lucide-react";
import { useState } from "react";
import { useEffect } from "react";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";

interface FooterProps {
  selectedCount: number;
  isExporting: boolean;
  isCanceling: boolean;
  currentBook?: string;
  currentStep?: string;
  completed: number;
  onExport: () => void;
  onCancel: () => void;
}

export function Footer({
  selectedCount,
  isExporting,
  isCanceling,
  currentBook,
  currentStep,
  completed,
  onExport,
  onCancel,
}: FooterProps) {
  const isOnline = useNetworkState();
  const [showCancelDialog, setShowCancelDialog] = useState(false);
  const [isHovered, setIsHovered] = useState(false);

  const handleCancelClick = () => {
    setShowCancelDialog(true);
  };

  const handleConfirmCancel = () => {
    setShowCancelDialog(false);
    onCancel();
  };

  return (
    <>
      <div
        id="footer"
        className="w-4/5 fixed bottom-0 bg-background backdrop-blur-none border-t"
      >
        <div className="container mx-auto px-4 h-16 flex items-center justify-between">
          <div className="flex flex-col gap-2 flex-1 mr-4">
            {!isOnline.online ? (
              <div className="flex items-center space-x-2 text-destructive/95">
                <AlertCircle className="h-4 w-4" />
                <span className="text-md font-bold">
                  Network is not available. Please check your network settings.
                </span>
              </div>
            ) : isExporting ? (
              <>
                <div className="flex justify-between text-sm text-muted-foreground">
                  <span className="font-bold">{currentBook}</span>
                  {isCanceling ? (
                    <span className="font-bold">Aborting the export...</span>
                  ) : (
                    <span className="font-bold">{currentStep}</span>
                  )}
                </div>
                <Progress value={(completed / selectedCount) * 100} />
              </>
            ) : (
              <div className="text-md font-bold">
                {selectedCount > 0
                  ? `${selectedCount} book${selectedCount > 1 ? "s" : ""} selected`
                  : "Select books to export"}
              </div>
            )}
          </div>
          <Button
            className={`text-md font-bold ${!isOnline.online ? "bg-destructive/60" : ""}`}
            onClick={isExporting && !isCanceling ? handleCancelClick : onExport}
            disabled={selectedCount === 0 || !isOnline.online || isCanceling}
            onMouseEnter={() => setIsHovered(true)}
            onMouseLeave={() => setIsHovered(false)}
          >
            {!isOnline.online ? (
              "No Network Connection ;("
            ) : isExporting ? (
              <div className="flex items-center space-x-2 justify-center">
                {isCanceling ? (
                  <div className="flex items-center justify-center space-x-2 w-[120px]">
                    <Loader2 className="h-4 w-4 animate-spin" />
                    <span>Canceling...</span>
                  </div>
                ) : isHovered ? (
                  <div className="flex items-center justify-center space-x-2 w-[120px]">
                    <XCircle className="h-4 w-4" />
                    <span>Cancel</span>
                  </div>
                ) : (
                  <div className="flex items-center justify-center space-x-2 w-[120px]">
                    <Loader2 className="h-4 w-4 animate-spin" />
                    <span>Exporting...</span>
                  </div>
                )}
              </div>
            ) : (
              "Export to Notion"
            )}
          </Button>
        </div>
      </div>

      <AlertDialog open={showCancelDialog} onOpenChange={setShowCancelDialog}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Cancel Export</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to cancel the export process? Current progress will be lost.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Continue Exporting</AlertDialogCancel>
            <AlertDialogAction onClick={handleConfirmCancel}>
              Yes, Cancel Export
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}
