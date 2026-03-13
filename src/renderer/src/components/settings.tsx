import { useState, useEffect, useRef } from 'react'
import { Input } from '@/components/ui/input'
import { PasswordInput } from '@/components/ui/password-input'
import { Separator } from '@/components/ui/separator'
import { Button } from '@/components/ui/button'
import { Switch } from '@/components/ui/switch'
import { useToast } from '@/hooks/use-toast'
import { ToastAction } from '@/components/ui/toast'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '@/components/ui/select'
import { FolderOpen, RefreshCw } from 'lucide-react'
import { ScrollArea } from '@/components/ui/scroll-area'

interface SettingsValues {
  SQLITE_SOURCE: string
  NOTION_API: string
  NOTION_DB: string
  LLM_PROVIDER: string
  LLM_API_KEY_GOOGLE: string
  LLM_API_KEY_OPENAI: string
  LLM_API_KEY_ANTHROPIC: string
  LLM_MODEL: string
  SUMMARIZE_ENABLED: boolean
  SUMMARIZE_LANGUAGE: string
}

const providerLabels: Record<string, string> = {
  google: 'Google Gemini',
  openai: 'OpenAI',
  anthropic: 'Anthropic Claude'
}

const providerKeyField: Record<string, keyof SettingsValues> = {
  google: 'LLM_API_KEY_GOOGLE',
  openai: 'LLM_API_KEY_OPENAI',
  anthropic: 'LLM_API_KEY_ANTHROPIC'
}

export function Settings(): React.JSX.Element {
  const [values, setValues] = useState<SettingsValues>({
    SQLITE_SOURCE: window.env.SQLITE_SOURCE || '',
    NOTION_API: window.env.NOTION_API_KEY || '',
    NOTION_DB: window.env.NOTION_DATA_SOURCE_ID || '',
    LLM_PROVIDER: window.env.LLM_PROVIDER || '',
    LLM_API_KEY_GOOGLE: window.env.LLM_API_KEY_GOOGLE || '',
    LLM_API_KEY_OPENAI: window.env.LLM_API_KEY_OPENAI || '',
    LLM_API_KEY_ANTHROPIC: window.env.LLM_API_KEY_ANTHROPIC || '',
    LLM_MODEL: window.env.LLM_MODEL || '',
    SUMMARIZE_ENABLED: window.env.SUMMARIZE_ENABLED || false,
    SUMMARIZE_LANGUAGE: window.env.SUMMARIZE_LANGUAGE || 'en'
  })
  const [isSaving, setIsSaving] = useState(false)
  const [isFirstTime, setIsFirstTime] = useState(true)
  const [availableModels, setAvailableModels] = useState<string[]>([])
  const [isLoadingModels, setIsLoadingModels] = useState(false)
  const [modelsFetchError, setModelsFetchError] = useState(false)
  const { toast } = useToast()
  const debounceRef = useRef<NodeJS.Timeout | null>(null)

  // Derive the active API key for the current provider
  const activeApiKeyField = providerKeyField[values.LLM_PROVIDER]
  const activeApiKey = activeApiKeyField ? (values[activeApiKeyField] as string) : ''

  useEffect(() => {
    setIsFirstTime(!values.SQLITE_SOURCE && !values.NOTION_API && !values.NOTION_DB)
  }, [])

  // Fetch models when provider or its API key changes
  useEffect(() => {
    if (!values.LLM_PROVIDER || !activeApiKey) {
      setAvailableModels([])
      return
    }

    if (debounceRef.current) clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => {
      loadModels(values.LLM_PROVIDER, activeApiKey)
    }, 500)

    return (): void => {
      if (debounceRef.current) clearTimeout(debounceRef.current)
    }
  }, [values.LLM_PROVIDER, activeApiKey])

  const loadModels = async (provider: string, apiKey: string): Promise<void> => {
    setIsLoadingModels(true)
    setModelsFetchError(false)
    try {
      const models = await window.api.fetchAvailableModels(provider, apiKey)
      setAvailableModels(models)
      if (models.length > 0) {
        setValues((prev) => ({
          ...prev,
          LLM_MODEL: models.includes(prev.LLM_MODEL) ? prev.LLM_MODEL : models[0]
        }))
      }
    } catch {
      setModelsFetchError(true)
      setAvailableModels([])
    } finally {
      setIsLoadingModels(false)
    }
  }

  const validateSqlitePath = (path: string): boolean => {
    return path.toLowerCase().includes('koboreader.sqlite')
  }

  const handleChange = (key: string, value: string | boolean): void => {
    setValues((prev) => ({ ...prev, [key]: value }))
  }

  const handleProviderChange = (provider: string): void => {
    setValues((prev) => ({
      ...prev,
      LLM_PROVIDER: provider,
      LLM_MODEL: ''
    }))
    setAvailableModels([])
    setModelsFetchError(false)
  }

  const handleSummarizeToggle = (enabled: boolean): void => {
    setValues((prev) => ({
      ...prev,
      SUMMARIZE_ENABLED: enabled,
      LLM_PROVIDER: enabled ? prev.LLM_PROVIDER || 'google' : prev.LLM_PROVIDER
    }))
  }

  const isValid = (): boolean => {
    const requiredFields = [values.SQLITE_SOURCE, values.NOTION_API, values.NOTION_DB]

    if (values.SUMMARIZE_ENABLED) {
      requiredFields.push(activeApiKey)
    }

    return requiredFields.every((field) => field.trim() !== '')
  }

  const handleSave = async (): Promise<void> => {
    setIsSaving(true)
    try {
      // Build entries with LLM_API_KEY derived from the active provider's key
      const saveValues = {
        ...values,
        LLM_API_KEY: activeApiKey
      }
      const entries = Object.entries(saveValues).map(([key, value]) => ({
        key,
        value: typeof value === 'boolean' ? value.toString() : value
      }))
      await window.api.updateEnvValue(entries).then(() => {
        toast({
          title: 'Settings saved successfully',
          description: 'Please restart the app to apply changes',
          variant: 'default'
        })

        setTimeout(() => {
          window.location.reload()
        }, 500)
      })
    } catch (err) {
      toast({
        title: 'Failed to save settings',
        description: 'Please check your settings and try again',
        variant: 'destructive'
      })
      console.error('Error saving settings:', err)
    } finally {
      setIsSaving(false)
    }
  }

  const handleFilePick = async (): Promise<void> => {
    try {
      const filePath = await window.api.openFileDialog()
      if (filePath) {
        if (!validateSqlitePath(filePath)) {
          toast({
            title: 'Invalid file path',
            description: 'Must be KoboReader.sqlite',
            action: <ToastAction altText="Try again"> Try again</ToastAction>
          })
          return
        }
        handleChange('SQLITE_SOURCE', filePath)
      }
    } catch (err) {
      console.error('Error picking file:', err)
      toast({
        title: 'Failed to select file',
        description: 'Please try again',
        variant: 'destructive'
      })
    }
  }

  return (
    <>
      <ScrollArea className="h-[calc(100vh-8rem)]">
        <div className="flex justify-between items-center p-4 pb-0">
          <h1 className="text-2xl font-bold">Settings</h1>
        </div>
        <div className="p-4 flex justify-center mt-4 md:mt-8 lg:mt-12 2xl:mt-24">
          <div className="grid gap-6 w-full lg:w-1/2 xl:w-2/5">
            <div className="space-y-2">
              <label className="text-md font-medium">Kobo Highlights File Path</label>
              <div className="flex space-x-2">
                <Input
                  type="text"
                  value={values.SQLITE_SOURCE}
                  placeholder="/Volumes/KOBOeReader/.kobo/KoboReader.sqlite"
                  className={
                    !validateSqlitePath(values.SQLITE_SOURCE) && values.SQLITE_SOURCE
                      ? 'border-destructive'
                      : ''
                  }
                  readOnly
                />
                <Button variant="outline" size="icon" onClick={handleFilePick} title="Choose file">
                  <FolderOpen className="h-4 w-4" />
                </Button>
              </div>
              {values.SQLITE_SOURCE && !validateSqlitePath(values.SQLITE_SOURCE) && (
                <p className="text-sm text-destructive">File must be KoboReader.sqlite</p>
              )}
            </div>
            <Separator />
            <div className="space-y-2">
              <label className="text-md font-medium">Notion API Key</label>
              <PasswordInput
                value={values.NOTION_API}
                onChange={(e) => handleChange('NOTION_API', e.target.value)}
              />
            </div>
            <div className="space-y-2">
              <label className="text-md font-medium">Notion Data Source ID</label>
              <PasswordInput
                value={values.NOTION_DB}
                onChange={(e) => handleChange('NOTION_DB', e.target.value)}
              />
            </div>
            <Separator />
            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <label className="text-md font-medium">Summarize Bookmarks</label>
                <Switch
                  checked={values.SUMMARIZE_ENABLED}
                  onCheckedChange={handleSummarizeToggle}
                />
              </div>

              {values.SUMMARIZE_ENABLED && (
                <>
                  <div className="space-y-2">
                    <label className="text-md font-medium">Provider</label>
                    <Select value={values.LLM_PROVIDER} onValueChange={handleProviderChange}>
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {Object.entries(providerLabels).map(([value, label]) => (
                          <SelectItem key={value} value={value}>
                            {label}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>

                  {activeApiKeyField && (
                    <div className="space-y-2">
                      <label className="text-md font-medium">
                        {providerLabels[values.LLM_PROVIDER]} API Key
                      </label>
                      <PasswordInput
                        value={activeApiKey}
                        onChange={(e) => handleChange(activeApiKeyField, e.target.value)}
                      />
                    </div>
                  )}

                  <div className="space-y-2">
                    <div className="flex items-center justify-between">
                      <label className="text-md font-medium">Model</label>
                      {values.LLM_PROVIDER && activeApiKey && (
                        <Button
                          variant="ghost"
                          size="sm"
                          className="h-6 px-2 text-xs text-muted-foreground"
                          onClick={() => loadModels(values.LLM_PROVIDER, activeApiKey)}
                          disabled={isLoadingModels}
                        >
                          <RefreshCw
                            className={`h-3 w-3 mr-1 ${isLoadingModels ? 'animate-spin' : ''}`}
                          />
                          Refresh
                        </Button>
                      )}
                    </div>
                    {isLoadingModels ? (
                      <div className="flex items-center h-10 px-3 border rounded-md text-sm text-muted-foreground">
                        Fetching models...
                      </div>
                    ) : availableModels.length > 0 ? (
                      <Select
                        value={values.LLM_MODEL}
                        onValueChange={(value) => handleChange('LLM_MODEL', value)}
                      >
                        <SelectTrigger>
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {availableModels.map((model) => (
                            <SelectItem key={model} value={model}>
                              {model}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    ) : (
                      <div className="flex items-center h-10 px-3 border rounded-md text-sm text-muted-foreground">
                        {modelsFetchError
                          ? 'Failed to fetch models — check your API key'
                          : 'Enter an API key to load available models'}
                      </div>
                    )}
                  </div>

                  <div className="space-y-2">
                    <label className="text-md font-medium">Summary Language</label>
                    <Select
                      value={values.SUMMARIZE_LANGUAGE}
                      onValueChange={(value) => handleChange('SUMMARIZE_LANGUAGE', value)}
                    >
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="en">English</SelectItem>
                        <SelectItem value="zh">繁體中文 Traditional Chinese</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                </>
              )}
            </div>

            <Separator />
            <div className="flex justify-center space-x-2">
              {!isFirstTime && (
                <Button
                  variant="outline"
                  className="w-full text-md font-bold"
                  onClick={() => window.location.reload()}
                >
                  Cancel
                </Button>
              )}
              <Button
                className="w-full text-md font-bold"
                onClick={handleSave}
                disabled={!isValid() || isSaving}
              >
                {isSaving ? 'Saving...' : 'Save'}
              </Button>
            </div>
          </div>
        </div>
      </ScrollArea>
    </>
  )
}
