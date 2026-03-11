import { cn } from '@/lib/utils'
import iconUrl from '@/assets/icon.png'

interface LogoProps extends React.HTMLAttributes<HTMLDivElement> {}

export function Logo({ className, ...props }: LogoProps) {
  return (
    <div className={cn('flex items-center gap-2', className)} {...props}>
      <img src={iconUrl} alt="Kobo2Notion" className="h-8 w-8" />
      <span className="text-3xl font-bold">Kobo2Notion</span>
    </div>
  )
}
