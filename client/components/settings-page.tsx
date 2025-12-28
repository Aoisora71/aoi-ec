"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Switch } from "@/components/ui/switch"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Badge } from "@/components/ui/badge"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Textarea } from "@/components/ui/textarea"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { 
  Settings, 
  RefreshCw, 
  FileText, 
  Download, 
  Trash2, 
  Database,
  Activity,
  DollarSign,
  Server,
  Key,
  HardDrive,
  ListTree,
  Plus,
  Pencil,
  Loader2,
  Upload,
  Shield,
  Languages
} from "lucide-react"
import { useToast } from "@/hooks/use-toast"
import { apiService, SettingsData as ApiSettingsData, LogEntry, CategoryRecord, PrimaryCategoryRecord } from "@/lib/api-service"
import { ApiTestComponent } from "@/components/api-test"

type DomesticShippingCostSettings = {
  regular: number
  size60: number
  size80: number
  size100: number
}

interface SettingsData {
  // Pricing Settings
  exchangeRate: number
  profitMarginPercent: number
  salesCommissionPercent: number
  currency: string
  
  // Purchase Price Calculation Settings
  domesticShippingCosts: DomesticShippingCostSettings
  internationalShippingRate: number
  customsDutyRate: number
  
  // Server Settings
  autoRefresh: boolean
  refreshInterval: number
  apiTimeout: number
  maxRetries: number
  
  // Logging Settings
  loggingEnabled: boolean
  logLevel: 'info' | 'warning' | 'error' | 'debug'
  maxLogEntries: number
  logRetentionDays: number
  
  // Database Settings
  databaseUrl: string
  connectionPoolSize: number
  queryTimeout: number
  enableBackup: boolean
  
  // Login Information
  rakumartApiKey: string
  rakumartApiSecret: string
  enableApiKeyRotation: boolean
  sessionTimeout: number
  
  // User Login Information
  username: string
  email: string
  password: string
}

const PRIMARY_CATEGORY_NONE = "__none__"

// Translation Base Settings Component
function TranslationBaseSettings() {
  const [translationConfig, setTranslationConfig] = useState<any>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [isSaving, setIsSaving] = useState(false)
  const [activeSection, setActiveSection] = useState<string>("config")
  const { toast } = useToast()

  useEffect(() => {
    loadTranslationSettings()
  }, [])

  const loadTranslationSettings = async () => {
    setIsLoading(true)
    try {
      const response = await apiService.getTranslationSettings()
      if (response.success && response.data) {
        setTranslationConfig(response.data)
      } else {
        toast({
          title: "エラー",
          description: response.error || "翻訳設定の読み込みに失敗しました",
          variant: "destructive",
        })
      }
    } catch (error: any) {
      toast({
        title: "エラー",
        description: error.message || "翻訳設定の読み込みに失敗しました",
        variant: "destructive",
      })
    } finally {
      setIsLoading(false)
    }
  }

  const handleSave = async () => {
    if (!translationConfig) return
    
    setIsSaving(true)
    try {
      const response = await apiService.saveTranslationSettings(translationConfig)
      if (response.success) {
        toast({
          title: "成功",
          description: "翻訳設定を保存しました",
        })
        // Reload config
        await apiService.reloadTranslationConfig()
      } else {
        toast({
          title: "エラー",
          description: response.error || "翻訳設定の保存に失敗しました",
          variant: "destructive",
        })
      }
    } catch (error: any) {
      toast({
        title: "エラー",
        description: error.message || "翻訳設定の保存に失敗しました",
        variant: "destructive",
      })
    } finally {
      setIsSaving(false)
    }
  }

  const updateConfig = (path: string[], value: any) => {
    setTranslationConfig((prev: any) => {
      const newConfig = JSON.parse(JSON.stringify(prev))
      let current = newConfig
      for (let i = 0; i < path.length - 1; i++) {
        if (!current[path[i]]) {
          current[path[i]] = {}
        }
        current = current[path[i]]
      }
      current[path[path.length - 1]] = value
      return newConfig
    })
  }

  const updateDictionary = (key: string, source: string, target: string) => {
    setTranslationConfig((prev: any) => {
      const newConfig = JSON.parse(JSON.stringify(prev))
      if (!newConfig.translation_map) newConfig.translation_map = {}
      if (target === "") {
        delete newConfig.translation_map[source]
      } else {
        newConfig.translation_map[source] = target
      }
      return newConfig
    })
  }

  const updatePattern = (category: string, patterns: string[]) => {
    setTranslationConfig((prev: any) => {
      const newConfig = JSON.parse(JSON.stringify(prev))
      if (!newConfig.pattern_dictionary) newConfig.pattern_dictionary = {}
      newConfig.pattern_dictionary[category] = patterns
      return newConfig
    })
  }

  const addPatternCategory = (newCategory: string) => {
    setTranslationConfig((prev: any) => {
      const newConfig = JSON.parse(JSON.stringify(prev))
      if (!newConfig.pattern_dictionary) newConfig.pattern_dictionary = {}
      if (!newConfig.pattern_dictionary[newCategory]) {
        newConfig.pattern_dictionary[newCategory] = []
      }
      return newConfig
    })
  }

  const deletePatternCategory = (category: string) => {
    setTranslationConfig((prev: any) => {
      const newConfig = JSON.parse(JSON.stringify(prev))
      if (newConfig.pattern_dictionary && newConfig.pattern_dictionary[category]) {
        delete newConfig.pattern_dictionary[category]
      }
      return newConfig
    })
  }

  const renamePatternCategory = (oldCategory: string, newCategory: string) => {
    setTranslationConfig((prev: any) => {
      const newConfig = JSON.parse(JSON.stringify(prev))
      if (newConfig.pattern_dictionary && newConfig.pattern_dictionary[oldCategory]) {
        newConfig.pattern_dictionary[newCategory] = newConfig.pattern_dictionary[oldCategory]
        delete newConfig.pattern_dictionary[oldCategory]
      }
      return newConfig
    })
  }

  const updateRemovalPattern = (category: string, patterns: string[]) => {
    setTranslationConfig((prev: any) => {
      const newConfig = JSON.parse(JSON.stringify(prev))
      if (!newConfig.removal_patterns) newConfig.removal_patterns = {}
      newConfig.removal_patterns[category] = patterns
      return newConfig
    })
  }

  const addRemovalPatternCategory = (newCategory: string) => {
    setTranslationConfig((prev: any) => {
      const newConfig = JSON.parse(JSON.stringify(prev))
      if (!newConfig.removal_patterns) newConfig.removal_patterns = {}
      if (!newConfig.removal_patterns[newCategory]) {
        newConfig.removal_patterns[newCategory] = []
      }
      return newConfig
    })
  }

  const deleteRemovalPatternCategory = (category: string) => {
    setTranslationConfig((prev: any) => {
      const newConfig = JSON.parse(JSON.stringify(prev))
      if (newConfig.removal_patterns && newConfig.removal_patterns[category]) {
        delete newConfig.removal_patterns[category]
      }
      return newConfig
    })
  }

  const renameRemovalPatternCategory = (oldCategory: string, newCategory: string) => {
    setTranslationConfig((prev: any) => {
      const newConfig = JSON.parse(JSON.stringify(prev))
      if (newConfig.removal_patterns && newConfig.removal_patterns[oldCategory]) {
        newConfig.removal_patterns[newCategory] = newConfig.removal_patterns[oldCategory]
        delete newConfig.removal_patterns[oldCategory]
      }
      return newConfig
    })
  }

  const addAdvancedItem = (itemName: string) => {
    setTranslationConfig((prev: any) => {
      const newConfig = JSON.parse(JSON.stringify(prev))
      if (!newConfig[itemName]) {
        newConfig[itemName] = []
      }
      return newConfig
    })
  }

  const deleteAdvancedItem = (itemName: string) => {
    setTranslationConfig((prev: any) => {
      const newConfig = JSON.parse(JSON.stringify(prev))
      if (newConfig[itemName]) {
        delete newConfig[itemName]
      }
      return newConfig
    })
  }

  const renameAdvancedItem = (oldName: string, newName: string) => {
    setTranslationConfig((prev: any) => {
      const newConfig = JSON.parse(JSON.stringify(prev))
      if (newConfig[oldName]) {
        newConfig[newName] = newConfig[oldName]
        delete newConfig[oldName]
      }
      return newConfig
    })
  }

  if (isLoading) {
    return (
      <Card>
        <CardContent className="flex items-center justify-center py-8">
          <Loader2 className="h-6 w-6 animate-spin" />
        </CardContent>
      </Card>
    )
  }

  if (!translationConfig) {
    return (
      <Card>
        <CardContent className="py-8 text-center">
          <p className="text-muted-foreground">翻訳設定を読み込めませんでした</p>
          <Button onClick={loadTranslationSettings} className="mt-4">
            再読み込み
          </Button>
        </CardContent>
      </Card>
    )
  }

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>翻訳設定</CardTitle>
              <CardDescription>
                翻訳モジュールの設定を管理します。翻訳マップ、パターン辞書、削除パターンを編集できます。
              </CardDescription>
            </div>
            <div className="flex gap-2">
              <Button
                variant="outline"
                onClick={loadTranslationSettings}
                disabled={isLoading || isSaving}
                className="gap-2"
              >
                <RefreshCw className="h-4 w-4" />
                リロード
              </Button>
              <Button
                onClick={handleSave}
                disabled={isLoading || isSaving}
                className="gap-2"
              >
                {isSaving && <Loader2 className="h-4 w-4 animate-spin" />}
                保存
              </Button>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <Tabs value={activeSection} onValueChange={setActiveSection} className="w-full">
            <TabsList className="grid w-full grid-cols-5">
              <TabsTrigger value="config">基本設定</TabsTrigger>
              <TabsTrigger value="translation_map">翻訳マップ</TabsTrigger>
              <TabsTrigger value="pattern_dictionary">パターン辞書</TabsTrigger>
              <TabsTrigger value="removal_patterns">削除パターン</TabsTrigger>
              <TabsTrigger value="advanced">高度な設定</TabsTrigger>
            </TabsList>

            <TabsContent value="config" className="space-y-4 mt-4">
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label>API Key</Label>
                  <Input
                    type="password"
                    value={translationConfig.config?.api_key || ""}
                    onChange={(e) => updateConfig(["config", "api_key"], e.target.value)}
                    placeholder="DeepL API Key"
                  />
                </div>
                <div className="space-y-2">
                  <Label>Server URL</Label>
                  <Input
                    value={translationConfig.config?.server_url || ""}
                    onChange={(e) => updateConfig(["config", "server_url"], e.target.value)}
                    placeholder="https://api-free.deepl.com/v2/translate"
                  />
                </div>
                <div className="space-y-2">
                  <Label>Rate Limit Delay (秒)</Label>
                  <Input
                    type="number"
                    step="0.1"
                    value={translationConfig.config?.rate_limit_delay || 0.1}
                    onChange={(e) => updateConfig(["config", "rate_limit_delay"], parseFloat(e.target.value) || 0.1)}
                  />
                </div>
                <div className="space-y-2">
                  <Label>Max Retries</Label>
                  <Input
                    type="number"
                    value={translationConfig.config?.max_retries || 3}
                    onChange={(e) => updateConfig(["config", "max_retries"], parseInt(e.target.value) || 3)}
                  />
                </div>
                <div className="space-y-2">
                  <Label>Retry Delay (秒)</Label>
                  <Input
                    type="number"
                    step="0.1"
                    value={translationConfig.config?.retry_delay || 1.0}
                    onChange={(e) => updateConfig(["config", "retry_delay"], parseFloat(e.target.value) || 1.0)}
                  />
                </div>
                <div className="space-y-2">
                  <Label>Cache Max Size</Label>
                  <Input
                    type="number"
                    value={translationConfig.config?.cache_max_size || 10000}
                    onChange={(e) => updateConfig(["config", "cache_max_size"], parseInt(e.target.value) || 10000)}
                  />
                </div>
                <div className="space-y-2">
                  <Label>Max Bytes</Label>
                  <Input
                    type="number"
                    value={translationConfig.config?.max_bytes || 32}
                    onChange={(e) => updateConfig(["config", "max_bytes"], parseInt(e.target.value) || 32)}
                  />
                </div>
              </div>
            </TabsContent>

            <TabsContent value="translation_map" className="mt-4">
              <TranslationMapEditor
                translationMap={translationConfig.translation_map || {}}
                onUpdate={updateDictionary}
              />
            </TabsContent>

            <TabsContent value="pattern_dictionary" className="mt-4">
              <PatternDictionaryEditor
                patternDictionary={translationConfig.pattern_dictionary || {}}
                onUpdate={updatePattern}
                onAddCategory={addPatternCategory}
                onDeleteCategory={deletePatternCategory}
                onRenameCategory={renamePatternCategory}
              />
            </TabsContent>

            <TabsContent value="removal_patterns" className="mt-4">
              <RemovalPatternsEditor
                removalPatterns={translationConfig.removal_patterns || {}}
                onUpdate={updateRemovalPattern}
                onAddCategory={addRemovalPatternCategory}
                onDeleteCategory={deleteRemovalPatternCategory}
                onRenameCategory={renameRemovalPatternCategory}
              />
            </TabsContent>

            <TabsContent value="advanced" className="mt-4">
              <AdvancedSettingsEditor
                config={translationConfig}
                onUpdate={updateConfig}
                onAddItem={addAdvancedItem}
                onDeleteItem={deleteAdvancedItem}
                onRenameItem={renameAdvancedItem}
              />
            </TabsContent>
          </Tabs>
        </CardContent>
      </Card>
    </div>
  )
}

// Translation Map Editor Component
function TranslationMapEditor({ translationMap, onUpdate }: { translationMap: Record<string, string>, onUpdate: (key: string, source: string, target: string) => void }) {
  const [searchTerm, setSearchTerm] = useState("")
  const [newSource, setNewSource] = useState("")
  const [newTarget, setNewTarget] = useState("")

  const filteredEntries = Object.entries(translationMap).filter(([source]) =>
    source.toLowerCase().includes(searchTerm.toLowerCase()) ||
    translationMap[source].toLowerCase().includes(searchTerm.toLowerCase())
  )

  const handleAdd = () => {
    if (newSource && newTarget) {
      onUpdate("", newSource, newTarget)
      setNewSource("")
      setNewTarget("")
    }
  }

  const handleDelete = (source: string) => {
    onUpdate("", source, "")
  }

  return (
    <div className="space-y-4">
      <div className="flex gap-2">
        <Input
          placeholder="検索..."
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          className="flex-1"
        />
      </div>
      <div className="border rounded-lg">
        <ScrollArea className="h-[500px]">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-[200px]">中国語</TableHead>
                <TableHead>日本語</TableHead>
                <TableHead className="w-[100px]">操作</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filteredEntries.map(([source, target]) => (
                <TableRow key={source}>
                  <TableCell>{source}</TableCell>
                  <TableCell>
                    <Input
                      value={target}
                      onChange={(e) => onUpdate("", source, e.target.value)}
                      className="border-0 p-0 h-auto"
                    />
                  </TableCell>
                  <TableCell>
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => handleDelete(source)}
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </ScrollArea>
      </div>
      <div className="flex gap-2">
        <Input
          placeholder="中国語"
          value={newSource}
          onChange={(e) => setNewSource(e.target.value)}
        />
        <Input
          placeholder="日本語"
          value={newTarget}
          onChange={(e) => setNewTarget(e.target.value)}
        />
        <Button onClick={handleAdd}>
          <Plus className="h-4 w-4 mr-2" />
          追加
        </Button>
      </div>
      <p className="text-xs text-muted-foreground">
        合計 {Object.keys(translationMap).length} エントリ
      </p>
    </div>
  )
}

// Pattern Dictionary Editor Component
function PatternDictionaryEditor({ 
  patternDictionary, 
  onUpdate,
  onAddCategory,
  onDeleteCategory,
  onRenameCategory
}: { 
  patternDictionary: Record<string, string[]>
  onUpdate: (category: string, patterns: string[]) => void
  onAddCategory: (category: string) => void
  onDeleteCategory: (category: string) => void
  onRenameCategory: (oldCategory: string, newCategory: string) => void
}) {
  const [selectedCategory, setSelectedCategory] = useState<string>(Object.keys(patternDictionary)[0] || "")
  const [newCategoryName, setNewCategoryName] = useState("")
  const [editingCategory, setEditingCategory] = useState<string | null>(null)
  const [newCategoryNameForRename, setNewCategoryNameForRename] = useState("")

  const currentPatterns = patternDictionary[selectedCategory] || []
  const [patternsText, setPatternsText] = useState(currentPatterns.join("\n"))

  useEffect(() => {
    setPatternsText(currentPatterns.join("\n"))
  }, [selectedCategory, currentPatterns])

  useEffect(() => {
    if (!selectedCategory && Object.keys(patternDictionary).length > 0) {
      setSelectedCategory(Object.keys(patternDictionary)[0])
    }
  }, [patternDictionary, selectedCategory])

  const handleSave = () => {
    if (!selectedCategory) return
    const patterns = patternsText.split("\n").map(p => p.trim()).filter(p => p)
    onUpdate(selectedCategory, patterns)
  }

  const handleAddCategory = () => {
    if (newCategoryName && !patternDictionary[newCategoryName]) {
      onAddCategory(newCategoryName)
      setSelectedCategory(newCategoryName)
      setNewCategoryName("")
    }
  }

  const handleDeleteCategory = (category: string) => {
    if (Object.keys(patternDictionary).length <= 1) {
      return // Don't delete the last category
    }
    onDeleteCategory(category)
    if (selectedCategory === category) {
      const remaining = Object.keys(patternDictionary).filter(c => c !== category)
      setSelectedCategory(remaining[0] || "")
    }
  }

  const handleRenameCategory = (oldCategory: string) => {
    if (newCategoryNameForRename && !patternDictionary[newCategoryNameForRename]) {
      onRenameCategory(oldCategory, newCategoryNameForRename)
      setSelectedCategory(newCategoryNameForRename)
      setEditingCategory(null)
      setNewCategoryNameForRename("")
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2 flex-wrap">
        <Select value={selectedCategory} onValueChange={setSelectedCategory}>
          <SelectTrigger className="w-[200px]">
            <SelectValue placeholder="カテゴリを選択" />
          </SelectTrigger>
          <SelectContent>
            {Object.keys(patternDictionary).map((cat) => (
              <SelectItem key={cat} value={cat}>
                {cat}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Button onClick={handleSave}>保存</Button>
        <Button
          variant="outline"
          onClick={() => setEditingCategory(selectedCategory)}
          disabled={!selectedCategory}
        >
          <Pencil className="h-4 w-4 mr-2" />
          カテゴリ名を編集
        </Button>
        <Button
          variant="outline"
          onClick={() => handleDeleteCategory(selectedCategory)}
          disabled={!selectedCategory || Object.keys(patternDictionary).length <= 1}
        >
          <Trash2 className="h-4 w-4 mr-2" />
          カテゴリを削除
        </Button>
      </div>

      {editingCategory && (
        <div className="flex gap-2 items-center p-3 border rounded-lg bg-muted">
          <Input
            placeholder="新しいカテゴリ名"
            value={newCategoryNameForRename}
            onChange={(e) => setNewCategoryNameForRename(e.target.value)}
            className="flex-1"
          />
          <Button onClick={() => handleRenameCategory(editingCategory)}>
            名前を変更
          </Button>
          <Button variant="outline" onClick={() => {
            setEditingCategory(null)
            setNewCategoryNameForRename("")
          }}>
            キャンセル
          </Button>
        </div>
      )}

      <div className="flex gap-2 items-center p-3 border rounded-lg">
        <Input
          placeholder="新しいカテゴリ名"
          value={newCategoryName}
          onChange={(e) => setNewCategoryName(e.target.value)}
          className="flex-1"
        />
        <Button onClick={handleAddCategory}>
          <Plus className="h-4 w-4 mr-2" />
          カテゴリを追加
        </Button>
      </div>

      <div>
        <div className="flex items-center justify-between mb-2">
          <Label>{selectedCategory || "カテゴリを選択してください"}</Label>
          {selectedCategory && (
            <span className="text-xs text-muted-foreground">
              {currentPatterns.length} パターン
            </span>
          )}
        </div>
        <Textarea
          value={patternsText}
          onChange={(e) => setPatternsText(e.target.value)}
          rows={15}
          placeholder="1行に1つのパターンを入力"
          disabled={!selectedCategory}
        />
      </div>
    </div>
  )
}

// Removal Patterns Editor Component
function RemovalPatternsEditor({ 
  removalPatterns, 
  onUpdate,
  onAddCategory,
  onDeleteCategory,
  onRenameCategory
}: { 
  removalPatterns: Record<string, string[]>
  onUpdate: (category: string, patterns: string[]) => void
  onAddCategory: (category: string) => void
  onDeleteCategory: (category: string) => void
  onRenameCategory: (oldCategory: string, newCategory: string) => void
}) {
  const [selectedCategory, setSelectedCategory] = useState<string>(Object.keys(removalPatterns)[0] || "")
  const [newCategoryName, setNewCategoryName] = useState("")
  const [editingCategory, setEditingCategory] = useState<string | null>(null)
  const [newCategoryNameForRename, setNewCategoryNameForRename] = useState("")

  const currentPatterns = removalPatterns[selectedCategory] || []
  const [patternsText, setPatternsText] = useState(currentPatterns.join("\n"))

  useEffect(() => {
    setPatternsText(currentPatterns.join("\n"))
  }, [selectedCategory, currentPatterns])

  useEffect(() => {
    if (!selectedCategory && Object.keys(removalPatterns).length > 0) {
      setSelectedCategory(Object.keys(removalPatterns)[0])
    }
  }, [removalPatterns, selectedCategory])

  const handleSave = () => {
    if (!selectedCategory) return
    const patterns = patternsText.split("\n").map(p => p.trim()).filter(p => p)
    onUpdate(selectedCategory, patterns)
  }

  const handleAddCategory = () => {
    if (newCategoryName && !removalPatterns[newCategoryName]) {
      onAddCategory(newCategoryName)
      setSelectedCategory(newCategoryName)
      setNewCategoryName("")
    }
  }

  const handleDeleteCategory = (category: string) => {
    if (Object.keys(removalPatterns).length <= 1) {
      return // Don't delete the last category
    }
    onDeleteCategory(category)
    if (selectedCategory === category) {
      const remaining = Object.keys(removalPatterns).filter(c => c !== category)
      setSelectedCategory(remaining[0] || "")
    }
  }

  const handleRenameCategory = (oldCategory: string) => {
    if (newCategoryNameForRename && !removalPatterns[newCategoryNameForRename]) {
      onRenameCategory(oldCategory, newCategoryNameForRename)
      setSelectedCategory(newCategoryNameForRename)
      setEditingCategory(null)
      setNewCategoryNameForRename("")
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2 flex-wrap">
        <Select value={selectedCategory} onValueChange={setSelectedCategory}>
          <SelectTrigger className="w-[200px]">
            <SelectValue placeholder="カテゴリを選択" />
          </SelectTrigger>
          <SelectContent>
            {Object.keys(removalPatterns).map((cat) => (
              <SelectItem key={cat} value={cat}>
                {cat}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Button onClick={handleSave}>保存</Button>
        <Button
          variant="outline"
          onClick={() => setEditingCategory(selectedCategory)}
          disabled={!selectedCategory}
        >
          <Pencil className="h-4 w-4 mr-2" />
          カテゴリ名を編集
        </Button>
        <Button
          variant="outline"
          onClick={() => handleDeleteCategory(selectedCategory)}
          disabled={!selectedCategory || Object.keys(removalPatterns).length <= 1}
        >
          <Trash2 className="h-4 w-4 mr-2" />
          カテゴリを削除
        </Button>
      </div>

      {editingCategory && (
        <div className="flex gap-2 items-center p-3 border rounded-lg bg-muted">
          <Input
            placeholder="新しいカテゴリ名"
            value={newCategoryNameForRename}
            onChange={(e) => setNewCategoryNameForRename(e.target.value)}
            className="flex-1"
          />
          <Button onClick={() => handleRenameCategory(editingCategory)}>
            名前を変更
          </Button>
          <Button variant="outline" onClick={() => {
            setEditingCategory(null)
            setNewCategoryNameForRename("")
          }}>
            キャンセル
          </Button>
        </div>
      )}

      <div className="flex gap-2 items-center p-3 border rounded-lg">
        <Input
          placeholder="新しいカテゴリ名"
          value={newCategoryName}
          onChange={(e) => setNewCategoryName(e.target.value)}
          className="flex-1"
        />
        <Button onClick={handleAddCategory}>
          <Plus className="h-4 w-4 mr-2" />
          カテゴリを追加
        </Button>
      </div>

      <div>
        <div className="flex items-center justify-between mb-2">
          <Label>{selectedCategory || "カテゴリを選択してください"}</Label>
          {selectedCategory && (
            <span className="text-xs text-muted-foreground">
              {currentPatterns.length} パターン
            </span>
          )}
        </div>
        <Textarea
          value={patternsText}
          onChange={(e) => setPatternsText(e.target.value)}
          rows={15}
          placeholder="1行に1つのパターンを入力"
          disabled={!selectedCategory}
        />
      </div>
    </div>
  )
}

// Advanced Settings Editor Component
function AdvancedSettingsEditor({
  config,
  onUpdate,
  onAddItem,
  onDeleteItem,
  onRenameItem
}: {
  config: any
  onUpdate: (path: string[], value: any) => void
  onAddItem: (itemName: string) => void
  onDeleteItem: (itemName: string) => void
  onRenameItem: (oldName: string, newName: string) => void
}) {
  const [selectedItem, setSelectedItem] = useState<string>("chinese_brackets")
  const [newItemName, setNewItemName] = useState("")
  const [editingItem, setEditingItem] = useState<string | null>(null)
  const [newItemNameForRename, setNewItemNameForRename] = useState("")

  // Get all array-type items (excluding config, translation_map, pattern_dictionary, removal_patterns, unicode_ranges)
  const advancedItems = Object.keys(config || {}).filter(key => 
    Array.isArray(config[key]) && 
    !['config', 'translation_map', 'pattern_dictionary', 'removal_patterns', 'unicode_ranges'].includes(key)
  )

  const currentValue = config[selectedItem] || []
  const [valueText, setValueText] = useState(Array.isArray(currentValue) ? currentValue.join(", ") : "")

  useEffect(() => {
    const val = config[selectedItem] || []
    setValueText(Array.isArray(val) ? val.join(", ") : "")
  }, [selectedItem, config])

  useEffect(() => {
    if (!selectedItem && advancedItems.length > 0) {
      setSelectedItem(advancedItems[0])
    }
  }, [advancedItems, selectedItem])

  const handleSave = () => {
    if (!selectedItem) return
    const values = valueText.split(",").map(v => v.trim()).filter(v => v)
    onUpdate([selectedItem], values)
  }

  const handleAddItem = () => {
    if (newItemName && !config[newItemName]) {
      onAddItem(newItemName)
      setSelectedItem(newItemName)
      setNewItemName("")
    }
  }

  const handleDeleteItem = (itemName: string) => {
    if (advancedItems.length <= 1) {
      return // Don't delete the last item
    }
    onDeleteItem(itemName)
    if (selectedItem === itemName) {
      const remaining = advancedItems.filter(i => i !== itemName)
      setSelectedItem(remaining[0] || "")
    }
  }

  const handleRenameItem = (oldName: string) => {
    if (newItemNameForRename && !config[newItemNameForRename]) {
      onRenameItem(oldName, newItemNameForRename)
      setSelectedItem(newItemNameForRename)
      setEditingItem(null)
      setNewItemNameForRename("")
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2 flex-wrap">
        <Select value={selectedItem} onValueChange={setSelectedItem}>
          <SelectTrigger className="w-[250px]">
            <SelectValue placeholder="項目を選択" />
          </SelectTrigger>
          <SelectContent>
            {advancedItems.map((item) => (
              <SelectItem key={item} value={item}>
                {item}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Button onClick={handleSave}>保存</Button>
        <Button
          variant="outline"
          onClick={() => setEditingItem(selectedItem)}
          disabled={!selectedItem}
        >
          <Pencil className="h-4 w-4 mr-2" />
          項目名を編集
        </Button>
        <Button
          variant="outline"
          onClick={() => handleDeleteItem(selectedItem)}
          disabled={!selectedItem || advancedItems.length <= 1}
        >
          <Trash2 className="h-4 w-4 mr-2" />
          項目を削除
        </Button>
      </div>

      {editingItem && (
        <div className="flex gap-2 items-center p-3 border rounded-lg bg-muted">
          <Input
            placeholder="新しい項目名"
            value={newItemNameForRename}
            onChange={(e) => setNewItemNameForRename(e.target.value)}
            className="flex-1"
          />
          <Button onClick={() => handleRenameItem(editingItem)}>
            名前を変更
          </Button>
          <Button variant="outline" onClick={() => {
            setEditingItem(null)
            setNewItemNameForRename("")
          }}>
            キャンセル
          </Button>
        </div>
      )}

      <div className="flex gap-2 items-center p-3 border rounded-lg">
        <Input
          placeholder="新しい項目名"
          value={newItemName}
          onChange={(e) => setNewItemName(e.target.value)}
          className="flex-1"
        />
        <Button onClick={handleAddItem}>
          <Plus className="h-4 w-4 mr-2" />
          項目を追加
        </Button>
      </div>

      <div>
        <div className="flex items-center justify-between mb-2">
          <Label>{selectedItem || "項目を選択してください"}</Label>
          {selectedItem && (
            <span className="text-xs text-muted-foreground">
              {Array.isArray(config[selectedItem]) ? config[selectedItem].length : 0} エントリ
            </span>
          )}
        </div>
        <Textarea
          value={valueText}
          onChange={(e) => setValueText(e.target.value)}
          rows={8}
          placeholder="カンマ区切りで値を入力"
          disabled={!selectedItem}
        />
        <p className="text-xs text-muted-foreground mt-1">
          カンマ区切りで複数の値を入力できます
        </p>
      </div>
    </div>
  )
}
const CATEGORY_SIZE_NONE = "__size_none__"

const CATEGORY_SIZE_OPTIONS = [
  { value: "DM", label: "DM (30cm)", numeric: 30 },
  { value: "60", label: "サイズ60 (60cm)", numeric: 60 },
  { value: "80", label: "サイズ80 (80cm)", numeric: 80 },
  { value: "100", label: "サイズ100 (100cm)", numeric: 100 },
]

const SIZE_OPTION_TO_VALUE: Record<string, number> = CATEGORY_SIZE_OPTIONS.reduce(
  (acc, option) => {
    acc[option.value] = option.numeric
    return acc
  },
  {} as Record<string, number>
)

const getSizeOptionFromValue = (value?: number | null): string => {
  if (value == null) return ""
  const entry = CATEGORY_SIZE_OPTIONS.find(
    (option) => option.numeric === Number(value)
  )
  return entry ? entry.value : ""
}

const getSizeValueForOption = (option?: string): number | undefined => {
  if (!option || option === CATEGORY_SIZE_NONE) return undefined
  return SIZE_OPTION_TO_VALUE[option] ?? undefined
}

interface CategoryFormState {
  categoryName: string
  categoryIdsInput: string
  rakutenCategoryIdsInput: string
  primaryCategoryId: string
  weight: string
  length: string
  width: string
  height: string
  sizeOption: string
  sizeValue: string
}

const emptyCategoryForm: CategoryFormState = {
  categoryName: "",
  categoryIdsInput: "",
  rakutenCategoryIdsInput: "",
  primaryCategoryId: PRIMARY_CATEGORY_NONE,
  weight: "",
  length: "",
  width: "",
  height: "",
  sizeOption: "",
  sizeValue: "",
}

interface AttributeGroupFormState {
  name: string
  valuesInput: string
}

export function SettingsPage() {
  const [settings, setSettings] = useState<SettingsData>({
    // Pricing Settings
    exchangeRate: 20.0,
    profitMarginPercent: 5,
    salesCommissionPercent: 10,
    currency: 'JPY',
    
    // Purchase Price Calculation Settings
    domesticShippingCosts: {
      regular: 300, // Domestic shipping cost in JPY
      size60: 360,
      size80: 420,
      size100: 480,
    },
    internationalShippingRate: 17, // International shipping rate per kg in CNY
    customsDutyRate: 100, // Customs duty in JPY
    
    // Server Settings
    autoRefresh: false,
    refreshInterval: 300, // 5 minutes
    apiTimeout: 30,
    maxRetries: 3,
    
    // Logging Settings
    loggingEnabled: true,
    logLevel: 'info',
    maxLogEntries: 1000,
    logRetentionDays: 30,
    
    // Database Settings
    databaseUrl: '',
    connectionPoolSize: 10,
    queryTimeout: 30,
    enableBackup: true,
    
    // Login Information
    rakumartApiKey: '',
    rakumartApiSecret: '',
    enableApiKeyRotation: false,
    sessionTimeout: 3600, // 1 hour
    
    // User Login Information
    username: '',
    email: '',
    password: '',
  })

  const [logs, setLogs] = useState<LogEntry[]>([])
  const [isLoading, setIsLoading] = useState(false)
  const [categories, setCategories] = useState<CategoryRecord[]>([])
  const [primaryCategories, setPrimaryCategories] = useState<PrimaryCategoryRecord[]>([])
  const [isCategoryLoading, setIsCategoryLoading] = useState(false)
  const [isPrimaryLoading, setIsPrimaryLoading] = useState(false)
  const [isCategoryDialogOpen, setIsCategoryDialogOpen] = useState(false)
  const [isPrimaryDialogOpen, setIsPrimaryDialogOpen] = useState(false)
  const [categoryForm, setCategoryForm] = useState<CategoryFormState>(emptyCategoryForm)
  const [editingCategory, setEditingCategory] = useState<CategoryRecord | null>(null)
  const [isCategorySaving, setIsCategorySaving] = useState(false)
  const [isAttributeDialogOpen, setIsAttributeDialogOpen] = useState(false)
  const [attributeCategory, setAttributeCategory] = useState<CategoryRecord | null>(null)
  const [attributeGroups, setAttributeGroups] = useState<AttributeGroupFormState[]>([])
  const [attributeGenreId, setAttributeGenreId] = useState("")
  const [isAttributeSaving, setIsAttributeSaving] = useState(false)
  const [primaryFormName, setPrimaryFormName] = useState("")
  const [primaryFormDefaultCategoryIds, setPrimaryFormDefaultCategoryIds] = useState("")
  const [editingPrimaryCategory, setEditingPrimaryCategory] = useState<PrimaryCategoryRecord | null>(null)
  const [isPrimarySaving, setIsPrimarySaving] = useState(false)
  const [riskProducts, setRiskProducts] = useState<{
    high_risk: { keywords: string[]; category_ids: string[] }
    low_risk: { keywords: string[]; category_ids: string[] }
  }>({
    high_risk: { keywords: [], category_ids: [] },
    low_risk: { keywords: [], category_ids: [] }
  })
  const [isRiskProductsLoading, setIsRiskProductsLoading] = useState(false)
  const [isRiskProductsSaving, setIsRiskProductsSaving] = useState(false)
  const { toast } = useToast()

  // Load settings and logs on component mount
  useEffect(() => {
    loadSettings()
    loadLogs()
    loadCategories()
    loadPrimaryCategories()
    loadRiskProducts()
  }, [])

  const loadSettings = async () => {
    try {
      const response = await apiService.getSettings()
      if (response.success && response.settings) {
        // Convert API format to local format
        const apiSettings = response.settings as any
        const fallbackDomestic = Number(apiSettings.domestic_shipping_cost ?? 300) || 0
        const shippingCosts = apiSettings.domestic_shipping_costs ?? {}
        const toNumber = (value: any, fallback: number) => {
          const parsed = parseFloat(value)
          return Number.isFinite(parsed) ? parsed : fallback
        }
        const domesticShippingCosts: DomesticShippingCostSettings = {
          regular: toNumber(shippingCosts.regular, fallbackDomestic),
          size60: toNumber(shippingCosts.size60, fallbackDomestic),
          size80: toNumber(shippingCosts.size80, fallbackDomestic),
          size100: toNumber(shippingCosts.size100, fallbackDomestic),
        }
        
        setSettings({
          // Pricing Settings
          exchangeRate: apiSettings.exchange_rate ?? 20.0,
          profitMarginPercent: apiSettings.profit_margin_percent ?? 5,
          salesCommissionPercent: apiSettings.sales_commission_percent ?? 10,
          currency: apiSettings.currency ?? 'JPY',
          
          // Purchase Price Calculation Settings
          domesticShippingCosts,
          internationalShippingRate: apiSettings.international_shipping_rate ?? 17,
          customsDutyRate: apiSettings.customs_duty_rate ?? 100,
          
          // Server Settings
          autoRefresh: response.settings.auto_refresh ?? false,
          refreshInterval: response.settings.refresh_interval ?? 300,
          apiTimeout: response.settings.api_timeout ?? 30,
          maxRetries: (response.settings as any).max_retries ?? 3,
          
          // Logging Settings
          loggingEnabled: response.settings.logging_enabled ?? true,
          logLevel: response.settings.log_level as 'info' | 'warning' | 'error' | 'debug' ?? 'info',
          maxLogEntries: response.settings.max_log_entries ?? 1000,
          logRetentionDays: (response.settings as any).log_retention_days ?? 30,
          
          // Database Settings
          databaseUrl: (response.settings as any).database_url ?? '',
          connectionPoolSize: (response.settings as any).connection_pool_size ?? 10,
          queryTimeout: (response.settings as any).query_timeout ?? 30,
          enableBackup: (response.settings as any).enable_backup ?? true,
          
          // Login Information
          rakumartApiKey: response.settings.rakumart_api_key || '',
          rakumartApiSecret: response.settings.rakumart_api_secret || '',
          enableApiKeyRotation: (response.settings as any).enable_api_key_rotation ?? false,
          sessionTimeout: (response.settings as any).session_timeout ?? 3600,
          
          // User Login Information
          username: (response.settings as any).username || '',
          email: (response.settings as any).email || '',
          password: (response.settings as any).password || '',
        })
      } else {
        // Backend not serving settings yet; keep defaults silently
        if (!response.success) {
          console.warn('Settings endpoint not available:', response.error)
        }
      }
    } catch (error) {
      console.error('Failed to load settings:', error)
      toast({
        title: "エラー",
        description: "設定の読み込みに失敗しました",
        variant: "destructive",
      })
    }
  }

  const loadLogs = async () => {
    try {
      const response = await apiService.getLogs(100) // Load last 100 logs
      if (response.success && response.logs) {
        setLogs(response.logs)
      } else {
        console.warn('Logs endpoint not available:', response.error)
      }
    } catch (error) {
      console.error('Failed to load logs:', error)
      toast({
        title: "エラー",
        description: "ログの読み込みに失敗しました",
        variant: "destructive",
      })
    }
  }

  const loadCategories = async () => {
    setIsCategoryLoading(true)
    try {
      const response = await apiService.getCategories()
      if (response.success && response.categories) {
        setCategories(response.categories)
      } else if (!response.success) {
        console.warn("Category endpoint not available:", response.error)
      }
    } catch (error) {
      console.error("Failed to load categories:", error)
      toast({
        title: "エラー",
        description: "カテゴリ情報の読み込みに失敗しました",
        variant: "destructive",
      })
    } finally {
      setIsCategoryLoading(false)
    }
  }

  const handleExportPrimaryCategories = async () => {
    try {
      const blob = await apiService.exportPrimaryCategories()
      const url = window.URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `primary_categories_export_${new Date().toISOString().slice(0, 10)}.xlsx`
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      window.URL.revokeObjectURL(url)
      toast({
        title: "成功",
        description: "メインカテゴリのエクスポートが完了しました",
      })
    } catch (error: any) {
      console.error('Failed to export primary categories:', error)
      toast({
        title: "エラー",
        description: error.message || "メインカテゴリのエクスポートに失敗しました",
        variant: "destructive",
      })
    }
  }

  const handleImportPrimaryCategories = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (!file) return

    if (!file.name.endsWith('.xlsx') && !file.name.endsWith('.xls')) {
      toast({
        title: "エラー",
        description: "XLSXファイルのみ対応しています",
        variant: "destructive",
      })
      return
    }

    try {
      const result = await apiService.importPrimaryCategories(file)
      if (result.success) {
        toast({
          title: "成功",
          description: result.message || `インポート完了: 新規${result.imported || 0}件、更新${result.updated || 0}件`,
        })
        loadPrimaryCategories()
      } else {
        toast({
          title: "エラー",
          description: result.error || "メインカテゴリのインポートに失敗しました",
          variant: "destructive",
        })
      }
    } catch (error: any) {
      console.error('Failed to import primary categories:', error)
      toast({
        title: "エラー",
        description: error.message || "メインカテゴリのインポートに失敗しました",
        variant: "destructive",
      })
    } finally {
      // Reset file input
      event.target.value = ''
    }
  }

  const handleExportCategories = async () => {
    try {
      const blob = await apiService.exportCategories()
      const url = window.URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `categories_export_${new Date().toISOString().slice(0, 10)}.xlsx`
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      window.URL.revokeObjectURL(url)
      toast({
        title: "成功",
        description: "カテゴリのエクスポートが完了しました",
      })
    } catch (error: any) {
      console.error('Failed to export categories:', error)
      toast({
        title: "エラー",
        description: error.message || "カテゴリのエクスポートに失敗しました",
        variant: "destructive",
      })
    }
  }

  const handleImportCategories = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (!file) return

    if (!file.name.endsWith('.xlsx') && !file.name.endsWith('.xls')) {
      toast({
        title: "エラー",
        description: "XLSXファイルのみ対応しています",
        variant: "destructive",
      })
      return
    }

    try {
      const result = await apiService.importCategories(file)
      if (result.success) {
        toast({
          title: "成功",
          description: result.message || `インポート完了: 新規${result.imported || 0}件、更新${result.updated || 0}件`,
        })
        loadCategories()
      } else {
        toast({
          title: "エラー",
          description: result.error || "カテゴリのインポートに失敗しました",
          variant: "destructive",
        })
      }
    } catch (error: any) {
      console.error('Failed to import categories:', error)
      toast({
        title: "エラー",
        description: error.message || "カテゴリのインポートに失敗しました",
        variant: "destructive",
      })
    } finally {
      // Reset file input
      event.target.value = ''
    }
  }

  const loadRiskProducts = async () => {
    setIsRiskProductsLoading(true)
    try {
      const response = await apiService.getRiskProducts()
      if (response.success && response.data) {
        setRiskProducts(response.data)
      } else {
        toast({
          title: "エラー",
          description: response.error || "リスク製品設定の読み込みに失敗しました",
          variant: "destructive",
        })
      }
    } catch (error: any) {
      toast({
        title: "エラー",
        description: error?.message || "リスク製品設定の読み込みに失敗しました",
        variant: "destructive",
      })
    } finally {
      setIsRiskProductsLoading(false)
    }
  }

  const handleSaveRiskProducts = async () => {
    setIsRiskProductsSaving(true)
    try {
      const response = await apiService.updateRiskProducts(riskProducts)
      if (response.success) {
        toast({
          title: "成功",
          description: "リスク製品設定が保存されました",
        })
      } else {
        toast({
          title: "エラー",
          description: response.error || "リスク製品設定の保存に失敗しました",
          variant: "destructive",
        })
      }
    } catch (error: any) {
      toast({
        title: "エラー",
        description: error?.message || "リスク製品設定の保存に失敗しました",
        variant: "destructive",
      })
    } finally {
      setIsRiskProductsSaving(false)
    }
  }

const loadPrimaryCategories = async () => {
  setIsPrimaryLoading(true)
  try {
    const response = await apiService.getPrimaryCategories()
    if (response.success && response.categories) {
      setPrimaryCategories(response.categories)
    } else if (!response.success) {
      console.warn("Primary category endpoint not available:", response.error)
    }
  } catch (error) {
    console.error("Failed to load primary categories:", error)
    toast({
      title: "エラー",
      description: "メインカテゴリ情報の読み込みに失敗しました",
      variant: "destructive",
    })
  } finally {
    setIsPrimaryLoading(false)
  }
}

  const handleCategoryDialogOpenChange = (open: boolean) => {
    setIsCategoryDialogOpen(open)
    if (!open) {
      setCategoryForm(emptyCategoryForm)
      setEditingCategory(null)
    }
  }

  const openAttributeDialog = (category: CategoryRecord) => {
    setAttributeCategory(category)
    setAttributeGenreId(category.genre_id ?? "")
    const existing = (category.attributes || []).map(attr => ({
      name: attr.name,
      valuesInput: (attr.values || []).join(", "),
    }))
    setAttributeGroups(existing.length > 0 ? existing : [{ name: "", valuesInput: "" }])
    setIsAttributeDialogOpen(true)
  }

  const handleAttributeDialogOpenChange = (open: boolean) => {
    setIsAttributeDialogOpen(open)
    if (!open) {
      setAttributeCategory(null)
      setAttributeGroups([])
      setAttributeGenreId("")
    }
  }

  const addAttributeGroup = () => {
    setAttributeGroups(prev => [...prev, { name: "", valuesInput: "" }])
  }

  const removeAttributeGroup = (index: number) => {
    setAttributeGroups(prev => prev.filter((_, i) => i !== index))
  }

  const updateAttributeGroup = (index: number, patch: Partial<AttributeGroupFormState>) => {
    setAttributeGroups(prev =>
      prev.map((g, i) => (i === index ? { ...g, ...patch } : g)),
    )
  }

  const handleAttributeSubmit = async () => {
    if (!attributeCategory) return

    const cleanGroups = attributeGroups
      .map(group => {
        const name = group.name.trim()
        if (!name) return null
        const values = group.valuesInput
          .split(/[\n,]+/)
          .map(v => v.trim())
          .filter(v => v.length > 0)
        return { name, values }
      })
      .filter((g): g is { name: string; values: string[] } => g !== null)

    setIsAttributeSaving(true)
    try {
      const response = await apiService.updateCategory(attributeCategory.id, {
        genre_id: attributeGenreId.trim() !== "" ? attributeGenreId.trim() : null,
        attributes: cleanGroups,
      })
      if (!response.success) {
        throw new Error(response.error || "属性の保存に失敗しました")
      }

      toast({
        title: "属性を保存しました",
        description: `カテゴリ「${attributeCategory.category_name}」の属性が更新されました`,
      })

      handleAttributeDialogOpenChange(false)
      await loadCategories()
    } catch (error) {
      console.error("Failed to save attributes:", error)
      toast({
        title: "エラー",
        description: error instanceof Error ? error.message : "属性の保存に失敗しました",
        variant: "destructive",
      })
    } finally {
      setIsAttributeSaving(false)
    }
  }

  const parseCategoryIds = (input: string) =>
    input
      .split(/[\s,]+/)
      .map((value) => value.trim())
      .filter((value) => value.length > 0)

  const parseNumericInput = (value: string): number | null => {
    if (!value || value.trim() === "") {
      return null
    }
    const parsed = parseFloat(value)
    return Number.isNaN(parsed) ? null : parsed
  }

  const handlePrimaryDialogOpenChange = (open: boolean) => {
    setIsPrimaryDialogOpen(open)
    if (!open) {
      setPrimaryFormName("")
      setEditingPrimaryCategory(null)
    }
  }

  const openCreatePrimaryDialog = () => {
    setEditingPrimaryCategory(null)
    setPrimaryFormName("")
    setPrimaryFormDefaultCategoryIds("")
    setIsPrimaryDialogOpen(true)
  }

  const openEditPrimaryDialog = (category: PrimaryCategoryRecord) => {
    setEditingPrimaryCategory(category)
    setPrimaryFormName(category.category_name)
    setPrimaryFormDefaultCategoryIds(category.default_category_ids?.join(", ") || "")
    setIsPrimaryDialogOpen(true)
  }

  const handlePrimarySubmit = async () => {
    const trimmedName = primaryFormName.trim()
    if (!trimmedName) {
      toast({
        title: "カテゴリ名を入力してください",
        variant: "destructive",
      })
      return
    }

    // Parse default category IDs (comma or newline separated)
    const defaultCategoryIds = primaryFormDefaultCategoryIds
      .split(/[,\n]/)
      .map(id => id.trim())
      .filter(id => id.length > 0)

    setIsPrimarySaving(true)
    try {
      const payload: { category_name: string; default_category_ids?: string[] } = {
        category_name: trimmedName,
      }
      if (defaultCategoryIds.length > 0) {
        payload.default_category_ids = defaultCategoryIds
      }

      const response = editingPrimaryCategory
        ? await apiService.updatePrimaryCategory(editingPrimaryCategory.id, payload)
        : await apiService.createPrimaryCategory(payload)

      if (!response.success) {
        throw new Error(response.error || "メインカテゴリの保存に失敗しました")
      }

      toast({
        title: editingPrimaryCategory ? "メインカテゴリを更新しました" : "メインカテゴリを追加しました",
        description: `カテゴリ「${trimmedName}」が${editingPrimaryCategory ? "更新" : "登録"}されました`,
      })

      handlePrimaryDialogOpenChange(false)
      await Promise.all([loadPrimaryCategories(), loadCategories()])
    } catch (error) {
      console.error("Failed to save primary category:", error)
      toast({
        title: "エラー",
        description: error instanceof Error ? error.message : "メインカテゴリの保存に失敗しました",
        variant: "destructive",
      })
    } finally {
      setIsPrimarySaving(false)
    }
  }

  const handlePrimaryDelete = async (category: PrimaryCategoryRecord) => {
    const confirmed = window.confirm(`メインカテゴリ「${category.category_name}」を削除しますか？`)
    if (!confirmed) {
      return
    }

    try {
      const response = await apiService.deletePrimaryCategory(category.id)
      if (!response.success) {
        throw new Error(response.error || "メインカテゴリの削除に失敗しました")
      }

      toast({
        title: "メインカテゴリを削除しました",
        description: `カテゴリ「${category.category_name}」が削除されました`,
      })

      await Promise.all([loadPrimaryCategories(), loadCategories()])
    } catch (error) {
      console.error("Failed to delete primary category:", error)
      toast({
        title: "エラー",
        description: error instanceof Error ? error.message : "メインカテゴリの削除に失敗しました",
        variant: "destructive",
      })
    }
  }

  const openCreateCategoryDialog = () => {
    setEditingCategory(null)
    setCategoryForm(emptyCategoryForm)
    setIsCategoryDialogOpen(true)
  }

  const openEditCategoryDialog = (category: CategoryRecord) => {
    setEditingCategory(category)
    setCategoryForm({
      categoryName: category.category_name,
      categoryIdsInput: category.category_ids.join(", "),
      rakutenCategoryIdsInput: (category.rakuten_category_ids || []).join(", "),
      primaryCategoryId: category.primary_category_id ? String(category.primary_category_id) : PRIMARY_CATEGORY_NONE,
      weight: category.weight?.toString() ?? "",
      length: category.length?.toString() ?? "",
      width: category.width?.toString() ?? "",
      height: category.height?.toString() ?? "",
      sizeOption: category.size_option ?? getSizeOptionFromValue(category.size),
      sizeValue: category.size?.toString() ?? "",
    })
    setIsCategoryDialogOpen(true)
  }

  const handleCategorySubmit = async () => {
    const trimmedName = categoryForm.categoryName.trim()
    const categoryIds = parseCategoryIds(categoryForm.categoryIdsInput)
    const rakutenCategoryIds = parseCategoryIds(categoryForm.rakutenCategoryIdsInput)

    if (!trimmedName) {
      toast({
        title: "カテゴリ名を入力してください",
        variant: "destructive",
      })
      return
    }

    if (categoryIds.length === 0) {
      toast({
        title: "カテゴリIDを1つ以上入力してください",
        description: "カンマまたは改行で複数IDを区切れます",
        variant: "destructive",
      })
      return
    }

    let primaryCategoryIdValue: number | null
    if (
      !categoryForm.primaryCategoryId ||
      categoryForm.primaryCategoryId === PRIMARY_CATEGORY_NONE
    ) {
      primaryCategoryIdValue = null
    } else {
      const parsed = Number.parseInt(categoryForm.primaryCategoryId, 10)
      primaryCategoryIdValue = Number.isNaN(parsed) ? null : parsed
    }

    const selectedSizeValue = getSizeValueForOption(categoryForm.sizeOption)
    const fallbackSizeValue =
      categoryForm.sizeValue.trim() !== ""
        ? parseNumericInput(categoryForm.sizeValue)
        : null

    const payload = {
      category_name: trimmedName,
      category_ids: categoryIds,
      rakuten_category_ids: rakutenCategoryIds.length > 0 ? rakutenCategoryIds : undefined,
      primary_category_id: primaryCategoryIdValue,
      weight: parseNumericInput(categoryForm.weight),
      length: parseNumericInput(categoryForm.length),
      width: parseNumericInput(categoryForm.width),
      height: parseNumericInput(categoryForm.height),
      size_option: categoryForm.sizeOption || null,
      size: selectedSizeValue ?? fallbackSizeValue,
    }

    setIsCategorySaving(true)
    try {
      const response = editingCategory
        ? await apiService.updateCategory(editingCategory.id, payload)
        : await apiService.createCategory(payload)

      if (!response.success) {
        throw new Error(response.error || "カテゴリの保存に失敗しました")
      }

      toast({
        title: editingCategory ? "カテゴリを更新しました" : "カテゴリを追加しました",
        description: editingCategory
          ? `カテゴリ「${trimmedName}」が更新されました`
          : `カテゴリ「${trimmedName}」が登録されました`,
      })

      handleCategoryDialogOpenChange(false)
      await loadCategories()
    } catch (error) {
      console.error("Failed to save category:", error)
      toast({
        title: "エラー",
        description: error instanceof Error ? error.message : "カテゴリの保存に失敗しました",
        variant: "destructive",
      })
    } finally {
      setIsCategorySaving(false)
    }
  }

  const handleCategoryDelete = async (category: CategoryRecord) => {
    const confirmed = window.confirm(`カテゴリ「${category.category_name}」を削除しますか？`)
    if (!confirmed) {
      return
    }

    try {
      const response = await apiService.deleteCategory(category.id)
      if (!response.success) {
        throw new Error(response.error || "カテゴリの削除に失敗しました")
      }

      toast({
        title: "カテゴリを削除しました",
        description: `カテゴリ「${category.category_name}」が削除されました`,
      })

      await loadCategories()
    } catch (error) {
      console.error("Failed to delete category:", error)
      toast({
        title: "エラー",
        description: error instanceof Error ? error.message : "カテゴリの削除に失敗しました",
        variant: "destructive",
      })
    }
  }

  const saveSettings = async () => {
    setIsLoading(true)
    try {
      // Convert local format to API format
      const apiSettings: ApiSettingsData = {
        // Pricing Settings
        exchange_rate: settings.exchangeRate,
        profit_margin_percent: settings.profitMarginPercent,
        sales_commission_percent: settings.salesCommissionPercent,
        currency: settings.currency,
        
        // Purchase Price Calculation Settings
        domestic_shipping_cost: settings.domesticShippingCosts.regular,
        domestic_shipping_costs: {
          regular: settings.domesticShippingCosts.regular,
          size60: settings.domesticShippingCosts.size60,
          size80: settings.domesticShippingCosts.size80,
          size100: settings.domesticShippingCosts.size100,
        },
        international_shipping_rate: settings.internationalShippingRate,
        customs_duty_rate: settings.customsDutyRate,
        
        // Server Settings
        auto_refresh: settings.autoRefresh,
        refresh_interval: settings.refreshInterval,
        api_timeout: settings.apiTimeout,
        max_retries: settings.maxRetries,
        
        // Logging Settings
        logging_enabled: settings.loggingEnabled,
        log_level: settings.logLevel,
        max_log_entries: settings.maxLogEntries,
        log_retention_days: settings.logRetentionDays,
        
        // Database Settings
        database_url: settings.databaseUrl,
        connection_pool_size: settings.connectionPoolSize,
        query_timeout: settings.queryTimeout,
        enable_backup: settings.enableBackup,
        
        // Login Information
        rakumart_api_key: settings.rakumartApiKey,
        rakumart_api_secret: settings.rakumartApiSecret,
        enable_api_key_rotation: settings.enableApiKeyRotation,
        session_timeout: settings.sessionTimeout,
        
        // User Login Information
        username: settings.username,
        email: settings.email,
        password: settings.password,
      }
      
      const response = await apiService.updateSettings(apiSettings)
      
      if (response.success) {
        toast({
          title: "成功",
          description: "設定が保存されました",
        })
        
        // Reload logs to show the new log entry
        loadLogs()
      } else {
        throw new Error(response.error || '設定の保存に失敗しました')
      }
    } catch (error) {
      console.error('Failed to save settings:', error)
      toast({
        title: "エラー",
        description: "設定の保存に失敗しました",
        variant: "destructive",
      })
    } finally {
      setIsLoading(false)
    }
  }

  const clearLogs = async () => {
    try {
      const response = await apiService.clearLogs()
      if (response.success) {
        setLogs([])
        toast({
          title: "成功",
          description: "ログがクリアされました",
        })
      } else {
        throw new Error(response.error || 'ログのクリアに失敗しました')
      }
    } catch (error) {
      console.error('Failed to clear logs:', error)
      toast({
        title: "エラー",
        description: "ログのクリアに失敗しました",
        variant: "destructive",
      })
    }
  }

  const exportLogs = () => {
    const logData = {
      exportedAt: new Date().toISOString(),
      logs: logs
    }
    
    const blob = new Blob([JSON.stringify(logData, null, 2)], { type: 'application/json' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `logs-${new Date().toISOString().split('T')[0]}.json`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    URL.revokeObjectURL(url)
    
    toast({
      title: "成功",
      description: "ログがエクスポートされました",
    })
  }

  const getLogLevelIcon = (level: string) => {
    switch (level) {
      case 'error':
        return <Activity className="h-4 w-4 text-destructive" />
      case 'warning':
        return <Activity className="h-4 w-4 text-yellow-500" />
      case 'success':
        return <Activity className="h-4 w-4 text-green-500" />
      case 'info':
      default:
        return <Activity className="h-4 w-4 text-blue-500" />
    }
  }

  const getLogLevelBadgeColor = (level: string) => {
    switch (level) {
      case 'error':
        return 'destructive'
      case 'warning':
        return 'secondary'
      case 'success':
        return 'default'
      case 'info':
      default:
        return 'outline'
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">設定</h1>
          <p className="text-muted-foreground">
            Rakumart商品管理システムの設定とログ管理
          </p>
        </div>
        <Button onClick={saveSettings} disabled={isLoading} className="gap-2">
          <Settings className="h-4 w-4" />
          {isLoading ? "保存中..." : "設定を保存"}
        </Button>
      </div>

      <Tabs defaultValue="pricing" className="space-y-6">
        <TabsList className="grid w-full grid-cols-8">
          <TabsTrigger value="pricing" className="gap-2">
            <DollarSign className="h-4 w-4" />
            価格設定
          </TabsTrigger>
          <TabsTrigger value="server" className="gap-2">
            <Server className="h-4 w-4" />
            サーバー設定
          </TabsTrigger>
          <TabsTrigger value="logging" className="gap-2">
            <FileText className="h-4 w-4" />
            ログ設定
          </TabsTrigger>
          <TabsTrigger value="categories" className="gap-2">
            <ListTree className="h-4 w-4" />
            カテゴリ管理
          </TabsTrigger>
          <TabsTrigger value="database" className="gap-2">
            <HardDrive className="h-4 w-4" />
            データベース
          </TabsTrigger>
          <TabsTrigger value="login" className="gap-2">
            <Key className="h-4 w-4" />
            ログイン情報
          </TabsTrigger>
          <TabsTrigger value="risk-products" className="gap-2">
            <Shield className="h-4 w-4" />
            規定設定
          </TabsTrigger>
          <TabsTrigger value="translation" className="gap-2">
            <Languages className="h-4 w-4" />
            翻訳設定
          </TabsTrigger>
        </TabsList>

        {/* Pricing Settings */}
        <TabsContent value="pricing">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <DollarSign className="h-5 w-5" />
                価格設定
              </CardTitle>
              <CardDescription>
                為替レートと通貨設定を管理します
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="grid gap-4 md:grid-cols-2">
                <div className="space-y-2">
                  <Label htmlFor="exchange-rate">為替レート (CNY → JPY)</Label>
                  <Input
                    id="exchange-rate"
                    type="number"
                    step="0.01"
                    value={settings.exchangeRate}
                    onChange={(e) => setSettings(prev => ({ ...prev, exchangeRate: parseFloat(e.target.value) || 0 }))}
                    placeholder="20.0"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="currency">通貨</Label>
                  <Select
                    value={settings.currency}
                    onValueChange={(value) => setSettings(prev => ({ ...prev, currency: value }))}
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="JPY">JPY (日本円)</SelectItem>
                      <SelectItem value="USD">USD (米ドル)</SelectItem>
                      <SelectItem value="EUR">EUR (ユーロ)</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
              
              <div className="border-t pt-6">
                <h3 className="text-lg font-semibold mb-4">購入価格計算設定</h3>
                <div className="grid gap-4 md:grid-cols-2">
                  <div className="space-y-2">
                    <Label htmlFor="profit-margin">利益率（%）</Label>
                    <Input
                      id="profit-margin"
                      type="number"
                      step="0.1"
                      value={settings.profitMarginPercent}
                      onChange={(e) => setSettings(prev => ({ ...prev, profitMarginPercent: parseFloat(e.target.value) || 0 }))}
                      placeholder="5"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="sales-commission">販売手数料率（%）</Label>
                    <Input
                      id="sales-commission"
                      type="number"
                      step="0.1"
                      value={settings.salesCommissionPercent}
                      onChange={(e) => setSettings(prev => ({ ...prev, salesCommissionPercent: parseFloat(e.target.value) || 0 }))}
                      placeholder="10"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label>国内配送料（サイズ別・円）</Label>
                    <div className="grid gap-4 md:grid-cols-2">
                      <div className="space-y-2">
                        <Label htmlFor="domestic-shipping-regular" className="text-xs text-muted-foreground">レギュラー</Label>
                        <Input
                          id="domestic-shipping-regular"
                          type="number"
                          value={settings.domesticShippingCosts.regular}
                          onChange={(e) => setSettings(prev => ({
                            ...prev,
                            domesticShippingCosts: { ...prev.domesticShippingCosts, regular: parseFloat(e.target.value) || 0 },
                          }))}
                          placeholder="300"
                        />
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="domestic-shipping-60" className="text-xs text-muted-foreground">サイズ60</Label>
                        <Input
                          id="domestic-shipping-60"
                          type="number"
                          value={settings.domesticShippingCosts.size60}
                          onChange={(e) => setSettings(prev => ({
                            ...prev,
                            domesticShippingCosts: { ...prev.domesticShippingCosts, size60: parseFloat(e.target.value) || 0 },
                          }))}
                          placeholder="360"
                        />
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="domestic-shipping-80" className="text-xs text-muted-foreground">サイズ80</Label>
                        <Input
                          id="domestic-shipping-80"
                          type="number"
                          value={settings.domesticShippingCosts.size80}
                          onChange={(e) => setSettings(prev => ({
                            ...prev,
                            domesticShippingCosts: { ...prev.domesticShippingCosts, size80: parseFloat(e.target.value) || 0 },
                          }))}
                          placeholder="420"
                        />
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="domestic-shipping-100" className="text-xs text-muted-foreground">サイズ100</Label>
                        <Input
                          id="domestic-shipping-100"
                          type="number"
                          value={settings.domesticShippingCosts.size100}
                          onChange={(e) => setSettings(prev => ({
                            ...prev,
                            domesticShippingCosts: { ...prev.domesticShippingCosts, size100: parseFloat(e.target.value) || 0 },
                          }))}
                          placeholder="480"
                        />
                      </div>
                    </div>
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="international-shipping">国際配送料率（元/kg）</Label>
                    <Input
                      id="international-shipping"
                      type="number"
                      step="0.1"
                      value={settings.internationalShippingRate}
                      onChange={(e) => setSettings(prev => ({ ...prev, internationalShippingRate: parseFloat(e.target.value) || 0 }))}
                      placeholder="17"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="customs-duty">関税（円）</Label>
                    <Input
                      id="customs-duty"
                      type="number"
                      value={settings.customsDutyRate}
                      onChange={(e) => setSettings(prev => ({ ...prev, customsDutyRate: parseFloat(e.target.value) || 0 }))}
                      placeholder="100"
                    />
                  </div>
                </div>
              </div>
              
              <div className="flex justify-end pt-4 border-t">
                <Button 
                  onClick={saveSettings} 
                  disabled={isLoading} 
                  className="gap-2"
                >
                  {isLoading ? (
                    <>
                      <Loader2 className="h-4 w-4 animate-spin" />
                      保存中...
                    </>
                  ) : (
                    <>
                      <RefreshCw className="h-4 w-4" />
                      価格設定を保存
                    </>
                  )}
                </Button>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        {/* Server Settings */}
        <TabsContent value="server">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Server className="h-5 w-5" />
                サーバー設定
              </CardTitle>
              <CardDescription>
                自動リフレッシュ、タイムアウト、リトライ設定を管理します
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="flex items-center justify-between">
                <div className="space-y-0.5">
                  <Label htmlFor="auto-refresh">自動リフレッシュ</Label>
                  <p className="text-sm text-muted-foreground">
                    定期的に商品データを自動更新します
                  </p>
                </div>
                <Switch
                  id="auto-refresh"
                  checked={settings.autoRefresh}
                  onCheckedChange={(checked) => setSettings(prev => ({ ...prev, autoRefresh: checked }))}
                />
              </div>

              {settings.autoRefresh && (
                <div className="space-y-2">
                  <Label htmlFor="refresh-interval">リフレッシュ間隔（秒）</Label>
                  <Select
                    value={settings.refreshInterval.toString()}
                    onValueChange={(value) => setSettings(prev => ({ ...prev, refreshInterval: parseInt(value) }))}
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="60">1分</SelectItem>
                      <SelectItem value="300">5分</SelectItem>
                      <SelectItem value="600">10分</SelectItem>
                      <SelectItem value="1800">30分</SelectItem>
                      <SelectItem value="3600">1時間</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              )}

              <div className="grid gap-4 md:grid-cols-2">
                <div className="space-y-2">
                  <Label htmlFor="api-timeout">APIタイムアウト（秒）</Label>
                  <Input
                    id="api-timeout"
                    type="number"
                    value={settings.apiTimeout}
                    onChange={(e) => setSettings(prev => ({ ...prev, apiTimeout: parseInt(e.target.value) || 30 }))}
                    placeholder="30"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="max-retries">最大リトライ回数</Label>
                  <Input
                    id="max-retries"
                    type="number"
                    value={settings.maxRetries}
                    onChange={(e) => setSettings(prev => ({ ...prev, maxRetries: parseInt(e.target.value) || 3 }))}
                    placeholder="3"
                  />
                </div>
              </div>
            </CardContent>
          </Card>
          <div className="mt-6">
            <ApiTestComponent />
          </div>
        </TabsContent>

        {/* Logging Settings */}
        <TabsContent value="logging">
          <div className="grid gap-6 md:grid-cols-2">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <FileText className="h-5 w-5" />
                  ログ設定
                </CardTitle>
                <CardDescription>
                  ログレベル、保持期間、最大エントリ数を設定します
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                <div className="flex items-center justify-between">
                  <div className="space-y-0.5">
                    <Label htmlFor="logging-enabled">ログ機能</Label>
                    <p className="text-sm text-muted-foreground">
                      システムログの記録を有効にします
                    </p>
                  </div>
                  <Switch
                    id="logging-enabled"
                    checked={settings.loggingEnabled}
                    onCheckedChange={(checked) => setSettings(prev => ({ ...prev, loggingEnabled: checked }))}
                  />
                </div>

                <div className="space-y-2">
                  <Label htmlFor="log-level">ログレベル</Label>
                  <Select
                    value={settings.logLevel}
                    onValueChange={(value) => setSettings(prev => ({ ...prev, logLevel: value as any }))}
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="debug">Debug</SelectItem>
                      <SelectItem value="info">Info</SelectItem>
                      <SelectItem value="warning">Warning</SelectItem>
                      <SelectItem value="error">Error</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div className="grid gap-4 md:grid-cols-2">
                  <div className="space-y-2">
                    <Label htmlFor="max-log-entries">最大ログエントリ数</Label>
                    <Input
                      id="max-log-entries"
                      type="number"
                      value={settings.maxLogEntries}
                      onChange={(e) => setSettings(prev => ({ ...prev, maxLogEntries: parseInt(e.target.value) || 1000 }))}
                      placeholder="1000"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="log-retention-days">ログ保持期間（日）</Label>
                    <Input
                      id="log-retention-days"
                      type="number"
                      value={settings.logRetentionDays}
                      onChange={(e) => setSettings(prev => ({ ...prev, logRetentionDays: parseInt(e.target.value) || 30 }))}
                      placeholder="30"
                    />
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Activity className="h-5 w-5" />
                  ログ管理
                </CardTitle>
                <CardDescription>
                  システムログの表示と管理
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="flex gap-2">
                  <Button variant="outline" size="sm" onClick={loadLogs} className="gap-2">
                    <RefreshCw className="h-4 w-4" />
                    更新
                  </Button>
                  <Button variant="outline" size="sm" onClick={exportLogs} className="gap-2">
                    <Download className="h-4 w-4" />
                    エクスポート
                  </Button>
                  <Button variant="outline" size="sm" onClick={clearLogs} className="gap-2">
                    <Trash2 className="h-4 w-4" />
                    クリア
                  </Button>
                </div>

                <ScrollArea className="h-96">
                  <div className="space-y-2">
                    {logs.length === 0 ? (
                      <div className="text-center text-muted-foreground py-8">
                        ログがありません
                      </div>
                    ) : (
                      logs.map((log, index) => (
                        <div key={`${log.id}-${index}`} className="flex items-start gap-3 p-3 border rounded-lg">
                          <div className="flex-shrink-0 mt-0.5">
                            {getLogLevelIcon(log.level)}
                          </div>
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center gap-2 mb-1">
                              <Badge variant={getLogLevelBadgeColor(log.level)}>
                                {log.level.toUpperCase()}
                              </Badge>
                              <span className="text-xs text-muted-foreground">
                                {new Date(log.timestamp).toLocaleString('ja-JP')}
                              </span>
                            </div>
                            <p className="text-sm font-medium">{log.message}</p>
                            {log.details && (
                              <p className="text-xs text-muted-foreground mt-1">{log.details}</p>
                            )}
                          </div>
                        </div>
                      ))
                    )}
                  </div>
                </ScrollArea>
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        {/* Category Management */}
        <TabsContent value="categories">
          <div className="space-y-6">
            <Card>
              <CardHeader>
                <div className="flex items-center justify-between">
                  <div>
                    <CardTitle className="flex items-center gap-2">
                      <ListTree className="h-5 w-5" />
                      メインカテゴリ管理
                    </CardTitle>
                    <CardDescription>
                      メインカテゴリ名を登録・編集して、カテゴリ分類の基準を作成します
                    </CardDescription>
                  </div>
                  <div className="flex gap-2">
                    <Button variant="outline" size="sm" onClick={loadPrimaryCategories} className="gap-2">
                      <RefreshCw className="h-4 w-4" />
                      更新
                    </Button>
                    <Button variant="outline" size="sm" onClick={handleExportPrimaryCategories} className="gap-2">
                      <Download className="h-4 w-4" />
                      エクスポート
                    </Button>
                    <label>
                      <input
                        type="file"
                        accept=".xlsx,.xls"
                        onChange={handleImportPrimaryCategories}
                        style={{ display: 'none' }}
                      />
                      <Button variant="outline" size="sm" asChild className="gap-2">
                        <span>
                          <Upload className="h-4 w-4" />
                          インポート
                        </span>
                      </Button>
                    </label>
                    <Button size="sm" className="gap-2" onClick={openCreatePrimaryDialog}>
                      <Plus className="h-4 w-4" />
                      メインカテゴリ追加
                    </Button>
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                <div className="border rounded-lg">
                  {isPrimaryLoading ? (
                    <div className="flex items-center justify-center gap-2 py-12 text-muted-foreground">
                      <Loader2 className="h-4 w-4 animate-spin" />
                      メインカテゴリを読み込み中...
                    </div>
                  ) : primaryCategories.length === 0 ? (
                    <div className="text-center text-muted-foreground py-12">
                      登録済みのメインカテゴリがありません。<br />
                      「メインカテゴリ追加」から新しいカテゴリを登録してください。
                    </div>
                  ) : (
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>メインカテゴリ名</TableHead>
                          <TableHead>デフォルトカテゴリID</TableHead>
                          <TableHead>作成日時</TableHead>
                          <TableHead>更新日時</TableHead>
                          <TableHead className="text-right">操作</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {primaryCategories.map((category) => (
                          <TableRow key={category.id}>
                            <TableCell>
                              <div className="font-semibold">{category.category_name}</div>
                              <p className="text-xs text-muted-foreground">ID: {category.id}</p>
                            </TableCell>
                            <TableCell>
                              {category.default_category_ids && category.default_category_ids.length > 0 ? (
                                <div className="flex flex-wrap gap-1">
                                  {category.default_category_ids.map((cid, idx) => (
                                    <Badge key={`${category.id}-${idx}-${cid}`} variant="outline">
                                      {cid}
                                    </Badge>
                                  ))}
                                </div>
                              ) : (
                                <span className="text-sm text-muted-foreground">未設定</span>
                              )}
                            </TableCell>
                            <TableCell className="text-sm text-muted-foreground">
                              {new Date(category.created_at).toLocaleString("ja-JP")}
                            </TableCell>
                            <TableCell className="text-sm text-muted-foreground">
                              {new Date(category.updated_at).toLocaleString("ja-JP")}
                            </TableCell>
                            <TableCell>
                              <div className="flex justify-end gap-2">
                                <Button
                                  variant="outline"
                                  size="sm"
                                  onClick={() => openEditPrimaryDialog(category)}
                                  className="gap-2"
                                >
                                  <Pencil className="h-4 w-4" />
                                  編集
                                </Button>
                                <Button
                                  variant="outline"
                                  size="sm"
                                  onClick={() => handlePrimaryDelete(category)}
                                  className="gap-2 text-destructive"
                                >
                                  <Trash2 className="h-4 w-4" />
                                  削除
                                </Button>
                              </div>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  )}
                </div>
              </CardContent>
            </Card>

            <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle className="flex items-center gap-2">
                    <ListTree className="h-5 w-5" />
                    カテゴリ管理
                  </CardTitle>
                  <CardDescription>
                    カテゴリIDと寸法情報を登録して、商品検索時に再利用できます
                  </CardDescription>
                </div>
                <div className="flex gap-2">
                  <Button variant="outline" size="sm" onClick={loadCategories} className="gap-2">
                    <RefreshCw className="h-4 w-4" />
                    更新
                  </Button>
                  <Button variant="outline" size="sm" onClick={handleExportCategories} className="gap-2">
                    <Download className="h-4 w-4" />
                    エクスポート
                  </Button>
                  <label>
                    <input
                      type="file"
                      accept=".xlsx,.xls"
                      onChange={handleImportCategories}
                      style={{ display: 'none' }}
                    />
                    <Button variant="outline" size="sm" asChild className="gap-2">
                      <span>
                        <Upload className="h-4 w-4" />
                        インポート
                      </span>
                    </Button>
                  </label>
                  <Button size="sm" className="gap-2" onClick={openCreateCategoryDialog}>
                    <Plus className="h-4 w-4" />
                    カテゴリ追加
                  </Button>
                </div>
              </div>
            </CardHeader>
            <CardContent>
              <div className="border rounded-lg">
                {isCategoryLoading ? (
                  <div className="flex items-center justify-center gap-2 py-12 text-muted-foreground">
                    <Loader2 className="h-4 w-4 animate-spin" />
                    カテゴリを読み込み中...
                  </div>
                ) : categories.length === 0 ? (
                  <div className="text-center text-muted-foreground py-12">
                    登録済みのカテゴリがありません。<br />
                    「カテゴリ追加」から新しいカテゴリを登録してください。
                  </div>
                ) : (
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>カテゴリ名</TableHead>
                        <TableHead>メインカテゴリ</TableHead>
                        <TableHead>カテゴリID</TableHead>
                        <TableHead>楽天カテゴリID</TableHead>
                        <TableHead>ジャンルID</TableHead>
                        <TableHead>重量・寸法</TableHead>
                        <TableHead>更新日時</TableHead>
                        <TableHead className="text-right">操作</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {categories.map((category) => (
                        <TableRow key={category.id}>
                          <TableCell>
                            <div className="font-semibold">{category.category_name}</div>
                            <p className="text-xs text-muted-foreground">ID: {category.id}</p>
                          </TableCell>
                          <TableCell>
                            {category.primary_category_name ? (
                              <Badge variant="secondary">{category.primary_category_name}</Badge>
                            ) : (
                              <span className="text-sm text-muted-foreground">未選択</span>
                            )}
                          </TableCell>
                          <TableCell>
                            <div className="flex flex-wrap gap-1">
                              {category.category_ids.map((cid) => (
                                <Badge key={`${category.id}-${cid}`} variant="outline">
                                  {cid}
                                </Badge>
                              ))}
                            </div>
                          </TableCell>
                          <TableCell>
                            {category.rakuten_category_ids && category.rakuten_category_ids.length > 0 ? (
                              <div className="flex flex-wrap gap-1">
                                {category.rakuten_category_ids.map((rid) => (
                                  <Badge key={`${category.id}-rakuten-${rid}`} variant="secondary">
                                    {rid}
                                  </Badge>
                                ))}
                              </div>
                            ) : (
                              <span className="text-sm text-muted-foreground">未設定</span>
                            )}
                          </TableCell>
                          <TableCell>
                            {category.genre_id ? (
                              <Badge variant="outline">{category.genre_id}</Badge>
                            ) : (
                              <span className="text-sm text-muted-foreground">未設定</span>
                            )}
                          </TableCell>
                          <TableCell>
                            <div className="text-sm leading-relaxed">
                              <div>重量: {category.weight ?? "-"} kg</div>
                              <div>長さ: {category.length ?? "-"} cm</div>
                              <div>幅: {category.width ?? "-"} cm</div>
                              <div>高さ: {category.height ?? "-"} cm</div>
                              <div>サイズ: {category.size ?? "-"} cm {category.size_option ? `(${category.size_option})` : ""}</div>
                            </div>
                          </TableCell>
                          <TableCell className="text-sm text-muted-foreground">
                            {new Date(category.updated_at).toLocaleString("ja-JP")}
                          </TableCell>
                          <TableCell>
                            <div className="flex justify-end gap-2">
                              <Button
                                variant="outline"
                                size="sm"
                                onClick={() => openAttributeDialog(category)}
                                className="gap-2"
                              >
                                <ListTree className="h-4 w-4" />
                                属性設定
                              </Button>
                              <Button
                                variant="outline"
                                size="sm"
                                onClick={() => openEditCategoryDialog(category)}
                                className="gap-2"
                              >
                                <Pencil className="h-4 w-4" />
                                編集
                              </Button>
                              <Button
                                variant="outline"
                                size="sm"
                                onClick={() => handleCategoryDelete(category)}
                                className="gap-2 text-destructive"
                              >
                                <Trash2 className="h-4 w-4" />
                                削除
                              </Button>
                            </div>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                )}
              </div>
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        {/* Database Settings */}
        <TabsContent value="database">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <HardDrive className="h-5 w-5" />
                データベース管理
              </CardTitle>
              <CardDescription>
                データベース接続設定とバックアップ管理
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="space-y-2">
                <Label htmlFor="database-url">データベースURL</Label>
                <Input
                  id="database-url"
                  type="password"
                  value={settings.databaseUrl}
                  onChange={(e) => setSettings(prev => ({ ...prev, databaseUrl: e.target.value }))}
                  placeholder="postgresql://user:password@162.43.44.223:5432/database"
                />
              </div>

              <div className="grid gap-4 md:grid-cols-2">
                <div className="space-y-2">
                  <Label htmlFor="connection-pool-size">接続プールサイズ</Label>
                  <Input
                    id="connection-pool-size"
                    type="number"
                    value={settings.connectionPoolSize}
                    onChange={(e) => setSettings(prev => ({ ...prev, connectionPoolSize: parseInt(e.target.value) || 10 }))}
                    placeholder="10"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="query-timeout">クエリタイムアウト（秒）</Label>
                  <Input
                    id="query-timeout"
                    type="number"
                    value={settings.queryTimeout}
                    onChange={(e) => setSettings(prev => ({ ...prev, queryTimeout: parseInt(e.target.value) || 30 }))}
                    placeholder="30"
                  />
                </div>
              </div>

              <div className="flex items-center justify-between">
                <div className="space-y-0.5">
                  <Label htmlFor="enable-backup">自動バックアップ</Label>
                  <p className="text-sm text-muted-foreground">
                    定期的にデータベースをバックアップします
                  </p>
                </div>
                <Switch
                  id="enable-backup"
                  checked={settings.enableBackup}
                  onCheckedChange={(checked) => setSettings(prev => ({ ...prev, enableBackup: checked }))}
                />
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        {/* Login Information */}
        <TabsContent value="login">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Key className="h-5 w-5" />
                ログイン情報
              </CardTitle>
              <CardDescription>
                APIキーとセッション設定を管理します
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="space-y-2">
                <Label htmlFor="rakumart-api-key">Rakumart API Key</Label>
                <Input
                  id="rakumart-api-key"
                  type="password"
                  value={settings.rakumartApiKey}
                  onChange={(e) => setSettings(prev => ({ ...prev, rakumartApiKey: e.target.value }))}
                  placeholder="APIキーを入力してください"
                />
              </div>

              <div className="space-y-2">
                <Label htmlFor="rakumart-api-secret">Rakumart API Secret</Label>
                <Input
                  id="rakumart-api-secret"
                  type="password"
                  value={settings.rakumartApiSecret}
                  onChange={(e) => setSettings(prev => ({ ...prev, rakumartApiSecret: e.target.value }))}
                  placeholder="APIシークレットを入力してください"
                />
              </div>

              <div className="grid gap-4 md:grid-cols-2">
                <div className="flex items-center justify-between">
                  <div className="space-y-0.5">
                    <Label htmlFor="api-key-rotation">APIキーローテーション</Label>
                    <p className="text-sm text-muted-foreground">
                      定期的にAPIキーを自動更新します
                    </p>
                  </div>
                  <Switch
                    id="api-key-rotation"
                    checked={settings.enableApiKeyRotation}
                    onCheckedChange={(checked) => setSettings(prev => ({ ...prev, enableApiKeyRotation: checked }))}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="session-timeout">セッションタイムアウト（秒）</Label>
                  <Input
                    id="session-timeout"
                    type="number"
                    value={settings.sessionTimeout}
                    onChange={(e) => setSettings(prev => ({ ...prev, sessionTimeout: parseInt(e.target.value) || 3600 }))}
                    placeholder="3600"
                  />
                </div>
              </div>
              
              <div className="border-t pt-6">
                <h3 className="text-lg font-semibold mb-4">ユーザーログイン情報</h3>
                <div className="grid gap-4 md:grid-cols-2">
                  <div className="space-y-2">
                    <Label htmlFor="username">ユーザー名</Label>
                    <Input
                      id="username"
                      type="text"
                      value={settings.username}
                      onChange={(e) => setSettings(prev => ({ ...prev, username: e.target.value }))}
                      placeholder="ユーザー名を入力してください"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="email">メールアドレス</Label>
                    <Input
                      id="email"
                      type="email"
                      value={settings.email}
                      onChange={(e) => setSettings(prev => ({ ...prev, email: e.target.value }))}
                      placeholder="メールアドレスを入力してください"
                    />
                  </div>
                  <div className="space-y-2 md:col-span-2">
                    <Label htmlFor="password">パスワード</Label>
                    <Input
                      id="password"
                      type="password"
                      value={settings.password}
                      onChange={(e) => setSettings(prev => ({ ...prev, password: e.target.value }))}
                      placeholder="パスワードを入力してください"
                    />
                  </div>
                </div>
                <div className="mt-4 p-4 bg-muted/50 rounded-lg">
                  <h4 className="font-medium mb-2">注意事項：</h4>
                  <p className="text-sm text-muted-foreground">
                    ユーザーログイン情報はプロジェクトのログインに使用されます。セキュリティのため、強力なパスワードを設定してください。
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        {/* Risk Products Settings Tab */}
        <TabsContent value="risk-products">
          <Card>
            <CardHeader>
              <CardTitle>規定設定</CardTitle>
              <CardDescription>
                高リスク製品と低リスク製品のキーワードおよびカテゴリIDを管理します
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              {isRiskProductsLoading ? (
                <div className="flex items-center justify-center py-8">
                  <Loader2 className="h-6 w-6 animate-spin" />
                </div>
              ) : (
                <>
                  {/* High Risk Products */}
                  <div className="space-y-4">
                    <div className="flex items-center justify-between">
                      <h3 className="text-lg font-semibold text-red-600">高リスク製品</h3>
                    </div>
                    
                    <div className="space-y-4">
                      <div>
                        <Label htmlFor="high-risk-keywords">高リスク製品キーワード</Label>
                        <Textarea
                          id="high-risk-keywords"
                          value={riskProducts.high_risk.keywords.join('\n')}
                          onChange={(e) => {
                            const keywords = e.target.value.split('\n').filter(k => k.trim())
                            setRiskProducts(prev => ({
                              ...prev,
                              high_risk: { ...prev.high_risk, keywords }
                            }))
                          }}
                          placeholder="1行に1つのキーワードを入力してください&#10;例:&#10;医薬品&#10;化粧品&#10;食品"
                          rows={6}
                          className="mt-2"
                        />
                        <p className="text-xs text-muted-foreground mt-1">
                          現在 {riskProducts.high_risk.keywords.length} 個のキーワードが登録されています
                        </p>
                      </div>
                      
                      <div>
                        <Label htmlFor="high-risk-category-ids">高リスク製品カテゴリID</Label>
                        <Textarea
                          id="high-risk-category-ids"
                          value={riskProducts.high_risk.category_ids.join('\n')}
                          onChange={(e) => {
                            const categoryIds = e.target.value.split('\n').filter(id => id.trim())
                            setRiskProducts(prev => ({
                              ...prev,
                              high_risk: { ...prev.high_risk, category_ids: categoryIds }
                            }))
                          }}
                          placeholder="1行に1つのカテゴリIDを入力してください&#10;例:&#10;100316&#10;100317&#10;100318"
                          rows={6}
                          className="mt-2"
                        />
                        <p className="text-xs text-muted-foreground mt-1">
                          現在 {riskProducts.high_risk.category_ids.length} 個のカテゴリIDが登録されています
                        </p>
                      </div>
                    </div>
                  </div>

                  <div className="border-t pt-6" />

                  {/* Low Risk Products */}
                  <div className="space-y-4">
                    <div className="flex items-center justify-between">
                      <h3 className="text-lg font-semibold text-yellow-600">低リスク製品</h3>
                    </div>
                    
                    <div className="space-y-4">
                      <div>
                        <Label htmlFor="low-risk-keywords">低リスク製品キーワード</Label>
                        <Textarea
                          id="low-risk-keywords"
                          value={riskProducts.low_risk.keywords.join('\n')}
                          onChange={(e) => {
                            const keywords = e.target.value.split('\n').filter(k => k.trim())
                            setRiskProducts(prev => ({
                              ...prev,
                              low_risk: { ...prev.low_risk, keywords }
                            }))
                          }}
                          placeholder="1行に1つのキーワードを入力してください&#10;例:&#10;日用品&#10;雑貨&#10;文具"
                          rows={6}
                          className="mt-2"
                        />
                        <p className="text-xs text-muted-foreground mt-1">
                          現在 {riskProducts.low_risk.keywords.length} 個のキーワードが登録されています
                        </p>
                      </div>
                      
                      <div>
                        <Label htmlFor="low-risk-category-ids">低リスク製品カテゴリID</Label>
                        <Textarea
                          id="low-risk-category-ids"
                          value={riskProducts.low_risk.category_ids.join('\n')}
                          onChange={(e) => {
                            const categoryIds = e.target.value.split('\n').filter(id => id.trim())
                            setRiskProducts(prev => ({
                              ...prev,
                              low_risk: { ...prev.low_risk, category_ids: categoryIds }
                            }))
                          }}
                          placeholder="1行に1つのカテゴリIDを入力してください&#10;例:&#10;100100&#10;100101&#10;100102"
                          rows={6}
                          className="mt-2"
                        />
                        <p className="text-xs text-muted-foreground mt-1">
                          現在 {riskProducts.low_risk.category_ids.length} 個のカテゴリIDが登録されています
                        </p>
                      </div>
                    </div>
                  </div>

                  <div className="flex justify-end gap-2 pt-4">
                    <Button
                      variant="outline"
                      onClick={loadRiskProducts}
                      disabled={isRiskProductsLoading || isRiskProductsSaving}
                      className="gap-2"
                    >
                      <RefreshCw className="h-4 w-4" />
                      リロード
                    </Button>
                    <Button
                      onClick={handleSaveRiskProducts}
                      disabled={isRiskProductsLoading || isRiskProductsSaving}
                      className="gap-2"
                    >
                      {isRiskProductsSaving && <Loader2 className="h-4 w-4 animate-spin" />}
                      保存
                    </Button>
                  </div>
                </>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Translation Base Settings */}
        <TabsContent value="translation">
          <TranslationBaseSettings />
        </TabsContent>
      </Tabs>

      <Dialog open={isCategoryDialogOpen} onOpenChange={handleCategoryDialogOpenChange}>
        <DialogContent className="max-w-lg">
          <DialogHeader>
            <DialogTitle>{editingCategory ? "カテゴリを編集" : "カテゴリを追加"}</DialogTitle>
            <DialogDescription>
              複数のカテゴリIDを登録できます。IDはJSON形式で保存され、手動で管理できます。
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="category-name">カテゴリ名</Label>
              <Input
                id="category-name"
                value={categoryForm.categoryName}
                onChange={(event) => setCategoryForm((prev) => ({ ...prev, categoryName: event.target.value }))}
                placeholder="例: レディースファッション"
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="primary-category">メインカテゴリ</Label>
              <Select
                value={categoryForm.primaryCategoryId}
                onValueChange={(value) =>
                  setCategoryForm((prev) => ({
                    ...prev,
                    primaryCategoryId: value,
                  }))
                }
              >
                <SelectTrigger id="primary-category">
                  <SelectValue placeholder="メインカテゴリを選択" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value={PRIMARY_CATEGORY_NONE}>未選択</SelectItem>
                  {primaryCategories.map((primary) => (
                    <SelectItem key={primary.id} value={String(primary.id)}>
                      {primary.category_name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-2">
              <Label htmlFor="category-ids">カテゴリID（複数可）</Label>
              <Textarea
                id="category-ids"
                rows={4}
                value={categoryForm.categoryIdsInput}
                onChange={(event) => setCategoryForm((prev) => ({ ...prev, categoryIdsInput: event.target.value }))}
                placeholder="1001, 1002, 1003"
              />
              <p className="text-sm text-muted-foreground">
                カンマまたは改行で区切って入力してください。登録時はJSON配列（例: [&quot;1001&quot;, &quot;1002&quot;]）として保存されます。
              </p>
            </div>

            <div className="space-y-2">
              <Label htmlFor="rakuten-category-ids">楽天カテゴリID（複数可）</Label>
              <Textarea
                id="rakuten-category-ids"
                rows={4}
                value={categoryForm.rakutenCategoryIdsInput}
                onChange={(event) => setCategoryForm((prev) => ({ ...prev, rakutenCategoryIdsInput: event.target.value }))}
                placeholder="100123, 100124, 100125"
              />
              <p className="text-sm text-muted-foreground">
                楽天市場のカテゴリIDをカンマまたは改行で区切って入力してください。複数のIDを登録できます。
              </p>
            </div>

            <div className="grid gap-4 md:grid-cols-2">
              <div className="space-y-2">
                <Label htmlFor="category-weight">重量 (kg)</Label>
                <Input
                  id="category-weight"
                  type="number"
                  step="0.1"
                  value={categoryForm.weight}
                  onChange={(event) => setCategoryForm((prev) => ({ ...prev, weight: event.target.value }))}
                  placeholder="1.2"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="category-length">長さ (cm)</Label>
                <Input
                  id="category-length"
                  type="number"
                  step="0.1"
                  value={categoryForm.length}
                  onChange={(event) => setCategoryForm((prev) => ({ ...prev, length: event.target.value }))}
                  placeholder="30"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="category-width">幅 (cm)</Label>
                <Input
                  id="category-width"
                  type="number"
                  step="0.1"
                  value={categoryForm.width}
                  onChange={(event) => setCategoryForm((prev) => ({ ...prev, width: event.target.value }))}
                  placeholder="20"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="category-height">高さ (cm)</Label>
                <Input
                  id="category-height"
                  type="number"
                  step="0.1"
                  value={categoryForm.height}
                  onChange={(event) => setCategoryForm((prev) => ({ ...prev, height: event.target.value }))}
                  placeholder="15"
                />
              </div>
              <div className="space-y-2">
                <Label>サイズ区分</Label>
                <Select
                  value={categoryForm.sizeOption || CATEGORY_SIZE_NONE}
                  onValueChange={(value) => {
                    if (value === CATEGORY_SIZE_NONE) {
                      setCategoryForm((prev) => ({
                        ...prev,
                        sizeOption: "",
                        sizeValue: "",
                      }))
                      return
                    }
                    const numeric = getSizeValueForOption(value)
                    setCategoryForm((prev) => ({
                      ...prev,
                      sizeOption: value,
                      sizeValue: numeric !== undefined ? numeric.toString() : "",
                    }))
                  }}
                >
                  <SelectTrigger id="category-size">
                    <SelectValue placeholder="サイズを選択" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value={CATEGORY_SIZE_NONE}>未選択</SelectItem>
                    {CATEGORY_SIZE_OPTIONS.map((option) => (
                      <SelectItem key={option.value} value={option.value}>
                        {option.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <Input
                  id="category-size-value"
                  type="number"
                  value={categoryForm.sizeValue}
                  readOnly
                  placeholder="選択すると自動入力"
                />
              </div>
            </div>
          </div>
          <DialogFooter className="gap-2 sm:gap-0">
            <Button variant="outline" onClick={() => handleCategoryDialogOpenChange(false)}>
              キャンセル
            </Button>
            <Button onClick={handleCategorySubmit} disabled={isCategorySaving} className="gap-2">
              {isCategorySaving && <Loader2 className="h-4 w-4 animate-spin" />}
              {editingCategory ? "カテゴリを更新" : "カテゴリを追加"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={isAttributeDialogOpen} onOpenChange={handleAttributeDialogOpenChange}>
        <DialogContent className="max-w-lg">
          <DialogHeader>
            <DialogTitle>
              属性設定 {attributeCategory ? `（${attributeCategory.category_name}）` : ""}
            </DialogTitle>
            <DialogDescription>
              属性名と値の組み合わせを複数設定できます。例：ブランド名・シリーズ名・原産国など。
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="attribute-genre-id">ジャンルID</Label>
              <Input
                id="attribute-genre-id"
                value={attributeGenreId}
                onChange={(e) => setAttributeGenreId(e.target.value)}
                placeholder="例: 100123"
              />
              <p className="text-xs text-muted-foreground">ジャンルIDを入力してください。空欄の場合は未設定のまま保存されます。</p>
            </div>
            {attributeGroups.length === 0 && (
              <div className="text-sm text-muted-foreground">
                まだ属性が設定されていません。「属性を追加」ボタンから新しい属性を追加してください。
              </div>
            )}
            {attributeGroups.map((group, index) => (
              <div key={index} className="border rounded-lg p-3 space-y-2">
                <div className="flex items-center justify-between gap-2">
                  <div className="flex-1 space-y-1">
                    <Label>属性名</Label>
                    <Input
                      value={group.name}
                      onChange={(e) =>
                        updateAttributeGroup(index, { name: e.target.value })
                      }
                      placeholder="例: ブランド名"
                    />
                  </div>
                  <Button
                    variant="outline"
                    size="icon"
                    className="mt-6 text-destructive"
                    onClick={() => removeAttributeGroup(index)}
                    disabled={attributeGroups.length <= 1}
                  >
                    <Trash2 className="h-4 w-4" />
                  </Button>
                </div>
                <div className="space-y-1">
                  <Label>値（複数可）</Label>
                  <Textarea
                    rows={3}
                    value={group.valuesInput}
                    onChange={(e) =>
                      updateAttributeGroup(index, { valuesInput: e.target.value })
                    }
                    placeholder="例: LICEL, その他ブランド名&#10;（カンマまたは改行で複数の値を区切ります）"
                  />
                </div>
              </div>
            ))}
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="gap-2"
              onClick={addAttributeGroup}
            >
              <Plus className="h-4 w-4" />
              属性を追加
            </Button>
          </div>
          <DialogFooter className="gap-2 sm:gap-0">
            <Button
              variant="outline"
              onClick={() => handleAttributeDialogOpenChange(false)}
            >
              キャンセル
            </Button>
            <Button
              onClick={handleAttributeSubmit}
              disabled={isAttributeSaving}
              className="gap-2"
            >
              {isAttributeSaving && <Loader2 className="h-4 w-4 animate-spin" />}
              属性を保存
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={isPrimaryDialogOpen} onOpenChange={handlePrimaryDialogOpenChange}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>{editingPrimaryCategory ? "メインカテゴリを編集" : "メインカテゴリを追加"}</DialogTitle>
            <DialogDescription>
              メインカテゴリ名のみを登録します。セカンダリカテゴリ登録時に選択できるようになります。
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="primary-category-name">メインカテゴリ名</Label>
              <Input
                id="primary-category-name"
                value={primaryFormName}
                onChange={(event) => setPrimaryFormName(event.target.value)}
                placeholder="例: ファッション"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="primary-default-category-ids">デフォルトカテゴリID</Label>
              <Textarea
                id="primary-default-category-ids"
                value={primaryFormDefaultCategoryIds}
                onChange={(event) => setPrimaryFormDefaultCategoryIds(event.target.value)}
                placeholder="例: 100316, 100317, 100318&#10;（カンマまたは改行で複数のIDを区切ります）"
                rows={4}
              />
              <p className="text-xs text-muted-foreground">
                複数のデフォルトカテゴリIDを設定できます。カンマまたは改行で区切って入力してください。
              </p>
            </div>
          </div>
          <DialogFooter className="gap-2 sm:gap-0">
            <Button variant="outline" onClick={() => handlePrimaryDialogOpenChange(false)}>
              キャンセル
            </Button>
            <Button onClick={handlePrimarySubmit} disabled={isPrimarySaving} className="gap-2">
              {isPrimarySaving && <Loader2 className="h-4 w-4 animate-spin" />}
              {editingPrimaryCategory ? "メインカテゴリを更新" : "メインカテゴリを追加"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
