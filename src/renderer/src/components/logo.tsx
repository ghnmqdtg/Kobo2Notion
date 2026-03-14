import { cn } from '@/lib/utils'
import { BookMarked } from 'lucide-react'

interface LogoProps extends React.HTMLAttributes<HTMLDivElement> {}

export function Logo({ className, ...props }: LogoProps): React.JSX.Element {
  return (
    <div className={cn('flex items-center gap-2', className)} {...props}>
      <BookMarked className="h-6 w-6" />
      <span className="text-xl font-bold font-mono">Kobo2Notion</span>
    </div>
  )
}
