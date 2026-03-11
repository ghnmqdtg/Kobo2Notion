import {
  AlertDialog,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogCancel,
  AlertDialogAction
} from '@/components/ui/alert-dialog'
import { UploadedPage } from '@/types'

interface DeletePagesDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  uploadedPages: UploadedPage[]
  onConfirm: () => void
  onCancel: () => void
}

export function DeletePagesDialog({
  open,
  onOpenChange,
  uploadedPages,
  onConfirm,
  onCancel
}: DeletePagesDialogProps) {
  return (
    <AlertDialog open={open} onOpenChange={onOpenChange}>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>Delete Uploaded Pages?</AlertDialogTitle>
          <AlertDialogDescription>
            Would you like to remove {uploadedPages.length} partially uploaded page
            {uploadedPages.length > 1 ? 's' : ''} from Notion?
            {uploadedPages.length > 0 && (
              <ul className="mt-2 space-y-1">
                {uploadedPages.map(({ bookTitle }) => (
                  <li key={bookTitle} className="text-sm">
                    • {bookTitle}
                  </li>
                ))}
              </ul>
            )}
          </AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel onClick={onCancel}>Keep Pages</AlertDialogCancel>
          <AlertDialogAction onClick={onConfirm}>Delete Pages</AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  )
}
