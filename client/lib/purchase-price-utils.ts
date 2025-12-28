// Purchase price calculation utility
export interface PurchasePriceSettings {
  exchangeRate: number
  domesticShippingCost: number
  internationalShippingRate: number
  customsDutyRate: number
  profitMarginPercent: number
  salesCommissionPercent: number
  domesticShippingCosts?: {
    regular?: number
    size60?: number
    size80?: number
    size100?: number
  }
}

const DEFAULT_EXCHANGE_RATE = 22

const SIZE_TO_DOMESTIC_KEY: Record<number, keyof NonNullable<PurchasePriceSettings["domesticShippingCosts"]>> = {
  30: "regular",
  60: "size60",
  80: "size80",
  100: "size100",
}

/**
 * Extract weightKg from product object's weight field.
 * The weight field comes from the product_origin table in the database.
 * 
 * @param product - Product object from database (products_origin table)
 * @returns Weight in kilograms (number), or null if not available
 * 
 * @example
 * const weightKg = getWeightKgFromProduct(product)
 * // Returns: 0.5 (if product.weight = 0.5) or null (if weight is missing)
 */
export function getWeightKgFromProduct(product: {
  weight?: number | string | null | undefined
}): number | null {
  if (!product || product.weight === null || product.weight === undefined) {
    return null
  }

  // Handle numeric weight
  if (typeof product.weight === 'number') {
    return Number.isFinite(product.weight) && product.weight >= 0 ? product.weight : null
  }

  // Handle string weight
  if (typeof product.weight === 'string') {
    const parsed = parseFloat(product.weight.trim())
    return Number.isFinite(parsed) && parsed >= 0 ? parsed : null
  }

  return null
}

export class WeightRequiredError extends Error {
  constructor(message: string = "Weight is required for purchase price calculation") {
    super(message)
    this.name = "WeightRequiredError"
  }
}

function resolveDomesticShippingCost(
  settings: PurchasePriceSettings,
  productSize?: number | null,
): number {
  const baseCost = Math.max(0, Number(settings.domesticShippingCost) || 0)
  const shippingCosts = settings.domesticShippingCosts
  if (!shippingCosts || productSize === null || productSize === undefined) {
    return baseCost
  }

  const sizeValue = Number(productSize)
  if (!Number.isFinite(sizeValue)) {
    return baseCost
  }

  const key = SIZE_TO_DOMESTIC_KEY[Math.round(sizeValue)]
  if (!key) {
    return baseCost
  }

  const tierCost = shippingCosts[key]
  if (typeof tierCost !== "number" || !Number.isFinite(tierCost)) {
    return baseCost
  }

  return Math.max(0, tierCost)
}

export function calculateActualPurchasePrice(
  productCostCny: number, // wholesale_price from DB (CNY)
  productWeightKg: number | null | undefined,
  settings: PurchasePriceSettings,
  productSize?: number | null,
): number {
  // Check if weight is null or undefined - throw error if missing
  if (productWeightKg === null || productWeightKg === undefined) {
    throw new WeightRequiredError("no weight")
  }

  const exchangeRate = settings.exchangeRate || DEFAULT_EXCHANGE_RATE
  const normalizedCost = Math.max(0, Number(productCostCny) || 0)
  const weightKg = Math.max(0, Number(productWeightKg) || 0)
  
  // Also check if weight is 0 or invalid after normalization
  if (weightKg <= 0 || !Number.isFinite(weightKg)) {
    throw new WeightRequiredError("no weight")
  }

  const profitMarginPercent = Number(settings.profitMarginPercent) || 0
  const salesCommissionPercent = Number(settings.salesCommissionPercent) || 0
  const internationalRate = Math.max(0, Number(settings.internationalShippingRate) || 0)
  const domesticShipping = resolveDomesticShippingCost(settings, productSize)

  const baseCost = normalizedCost * exchangeRate * 1.05
  const internationalShipping = internationalRate * weightKg * exchangeRate
  const numerator = baseCost + internationalShipping + domesticShipping

  const denominator = 100 - (profitMarginPercent + salesCommissionPercent)
  const safeDenominator = Math.abs(denominator) < 0.0001 ? 1 : denominator

  const actualPrice = (numerator * 100) / safeDenominator
  // Round to nearest 10 (set ones digit to 0)
  return Math.round(actualPrice / 10) * 10
}

export function formatPurchasePrice(price: number): string {
  return `¥${price.toLocaleString()}`
}
