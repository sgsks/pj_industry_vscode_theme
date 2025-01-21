// TypeScript Example - E-commerce Shopping Cart
import { Product, CartItem, Discount } from './types';
import { PaymentService } from './services/payment';
import { InventoryService } from './services/inventory';

interface IShoppingCart {
    addItem(product: Product, quantity: number): void;
    removeItem(productId: string): void;
    applyDiscount(code: string): Promise<boolean>;
    checkout(): Promise<boolean>;
}

export class ShoppingCart implements IShoppingCart {
    private items: Map<string, CartItem> = new Map();
    private activeDiscount?: Discount;

    constructor(
        private readonly paymentService: PaymentService,
        private readonly inventoryService: InventoryService,
    ) {}

    public addItem(product: Product, quantity: number): void {
        const existingItem = this.items.get(product.id);

        if (existingItem) {
            existingItem.quantity += quantity;
            existingItem.totalPrice = this.calculateItemPrice(existingItem);
        } else {
            const newItem: CartItem = {
                product,
                quantity,
                totalPrice: product.price * quantity
            };
            this.items.set(product.id, newItem);
        }
    }

    public async checkout(): Promise<boolean> {
        try {
            const total = this.calculateTotal();
            const inStock = await this.verifyInventory();

            if (!inStock) {
                throw new Error('Some items are out of stock');
            }

            const paymentResult = await this.paymentService.processPayment({
                amount: total,
                currency: 'USD',
                items: Array.from(this.items.values())
            });

            if (paymentResult.success) {
                await this.inventoryService.updateStock(this.items);
                this.clear();
                return true;
            }
            return false;
        } catch (error) {
            console.error('Checkout failed:', error);
            return false;
        }
    }

    private calculateTotal(): number {
        let total = Array.from(this.items.values())
            .reduce((sum, item) => sum + item.totalPrice, 0);

        if (this.activeDiscount) {
            total *= (1 - this.activeDiscount.percentage);
        }
        return Number(total.toFixed(2));
    }
}