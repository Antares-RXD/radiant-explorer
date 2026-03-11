import prisma from "@/lib/db";
import { getAddressData } from '@/lib/services/address';
import { Card } from '@/components/ui/Card';
import { TxIdDisplay } from "@/components/TxIdDisplay";
import Link from 'next/link';
import { AddressAsset, Transaction, TransactionInput, TransactionOutput, Address } from "@/types";

export const dynamic = 'force-dynamic';

export default async function AddressPage({ params, searchParams }: { params: Promise<{ address: string }>, searchParams: Promise<{ page?: string }> }) {
    const { address } = await params;
    const { page: pageParam } = await searchParams;
    const page = parseInt(pageParam || '1');
    const addrStr = address;

    let addr: Address | null = null;
    let assetBalances: AddressAsset[] = [];

    // Parallel data fetching
    try {
        const [addrData, assetsRes] = await Promise.all([
            getAddressData(addrStr, page, 50),
            prisma.addressAsset.findMany({
                where: {
                    address: addrStr,
                    balance: { gt: 0 }
                },
                include: { asset: true },
                orderBy: { balance: 'desc' }
            })
        ]);

        addr = addrData;

        // Use DB results directly
        assetBalances = assetsRes.map(row => ({
            asset: row.assetName,
            balance: Number(row.balance),
            units: row.asset?.units ?? 0
        }));

    } catch (e) {
        console.error("Error loading address data:", e);
    }

    if (!addr) {
        return <div className="text-center p-8 text-destructive">Address not found or error loading data.</div>;
    }

    function getTxNetAmount(tx: Transaction, address: string) {
        const received = (tx.vout || []).reduce((sum: number, v: TransactionOutput) => {
            const addresses = v?.scriptPubKey?.addresses || [];
            return addresses.includes(address) ? sum + (parseFloat(v.value) || 0) : sum;
        }, 0);
        const sent = (tx.vin || []).reduce((sum: number, v: TransactionInput) => {
            const addresses = v?.addresses || [];
            return addresses.includes(address) ? sum + (parseFloat(v.value || '0') || 0) : sum;
        }, 0);
        return received - sent;
    }

    // Fallback to Blockbook tokens if RPC fails or returns nothing, 
    // although Blockbook might not have full asset support as we discovered.
    // We will prioritize the RPC data if available.

    // NOTE: api.getAddress returns 'tokens' which might be empty if Blockbook isn't indexing assets.
    // We want to display the RPC fetched assets.

    return (
        <div className="flex flex-col gap-8 container mx-auto px-4 py-8">
            <div>
                <h1 className="text-3xl font-bold mb-2">Address</h1>
                <p className="text-muted-foreground break-all font-mono bg-muted/30 p-2 rounded inline-block">{addr.address}</p>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                <Card title="Overview">
                    <div className="p-6 grid gap-4">
                        <div className="flex justify-between items-center border-b border-border pb-2 last:border-0 last:pb-0">
                            <span className="text-muted-foreground text-sm font-medium">Balance</span>
                            <span className="font-mono">{parseFloat(addr.balance).toFixed(8)} <span className="text-primary">RXD</span></span>
                        </div>
                        <div className="flex justify-between items-center border-b border-border pb-2 last:border-0 last:pb-0">
                            <span className="text-muted-foreground text-sm font-medium">Total Received</span>
                            <span className="font-mono text-green-600 dark:text-green-400">{parseFloat(addr.totalReceived).toFixed(8)}</span>
                        </div>
                        <div className="flex justify-between items-center border-b border-border pb-2 last:border-0 last:pb-0">
                            <span className="text-muted-foreground text-sm font-medium">Total Sent</span>
                            <span className="font-mono text-red-600 dark:text-red-400">{parseFloat(addr.totalSent).toFixed(8)}</span>
                        </div>
                        <div className="flex justify-between items-center border-b border-border pb-2 last:border-0 last:pb-0">
                            <span className="text-muted-foreground text-sm font-medium">Transactions</span>
                            <span className="font-mono">{addr.txs}</span>
                        </div>
                    </div>
                </Card>

                <Card title="Assets Held">
                    <div className="p-6 max-h-[300px] overflow-y-auto space-y-3 custom-scrollbar">
                        {assetBalances.length > 0 ? (
                            assetBalances.map((asset: AddressAsset, i: number) => (
                                <div key={i} className="flex justify-between items-center p-3 bg-muted/20 rounded-lg hover:bg-muted/40 transition-colors">
                                    <Link href={`/asset/${asset.asset}`} className="font-medium text-primary hover:underline">
                                        {asset.asset}
                                    </Link>
                                    <span className="font-mono text-muted-foreground font-bold">
                                        {/* Balances from RPC are usually integers (satoshis equivalent), need to know decimals.
                                            Most Neurai assets have units. We might need to fetch asset def to know decimals...
                                            For now, let's display raw or assume 0 decimals if not known?
                                            Actually, usually listaddressbalances returns with decimals if formatted?
                                            Let's blindly display what RPC returns for now.
                                         */}
                                        {asset.balance}
                                    </span>
                                </div>
                            ))
                        ) : (
                            <div className="text-center text-muted-foreground text-sm">No assets held (or syncer lag).</div>
                        )}
                    </div>
                </Card>
            </div>

            <div className="space-y-4">
                <h2 className="text-2xl font-semibold">Transactions</h2>
                <div className="space-y-4">
                    {addr.transactions?.map((tx: Transaction) => {
                        const netAmount = getTxNetAmount(tx, addrStr);
                        const isIncoming = netAmount > 0;
                        const isOutgoing = netAmount < 0;
                        const amountClass = isIncoming
                            ? "text-green-600 dark:text-green-400"
                            : isOutgoing
                                ? "text-red-600 dark:text-red-400"
                                : "text-muted-foreground";
                        const amountLabel = `${isIncoming ? "+" : isOutgoing ? "-" : ""}${Math.abs(netAmount).toFixed(3)} RXD`;
                        const dateTime = new Date(tx.blocktime * 1000).toLocaleString(undefined, {
                            year: '2-digit',
                            month: 'numeric',
                            day: 'numeric',
                            hour: 'numeric',
                            minute: 'numeric',
                        });

                        return (
                            <Card key={tx.txid}>
                                <div className="p-4 flex flex-col lg:grid lg:grid-cols-[65%_35%] lg:items-center gap-3">
                                    <div className="flex flex-col min-w-0">
                                        <Link href={`/tx/${tx.txid}`} className="block min-w-0 overflow-hidden text-primary font-mono text-sm lg:text-base hover:underline">
                                            <TxIdDisplay txid={tx.txid} className="text-sm lg:text-base" />
                                        </Link>
                                    </div>
                                    <div className="hidden lg:flex flex-col items-end gap-1 pr-2">
                                        <span className={`font-mono font-bold ${amountClass}`}>{amountLabel}</span>
                                        <span className="text-sm text-muted-foreground">{dateTime}</span>
                                    </div>
                                    <div className="flex items-center justify-between text-sm text-muted-foreground lg:hidden">
                                        <span>{dateTime}</span>
                                        <span className={`font-mono font-bold ${amountClass}`}>{amountLabel}</span>
                                    </div>
                                </div>
                            </Card>
                        );
                    })}
                </div>
            </div>

            <div className="flex justify-between items-center mt-4 bg-muted/20 p-4 rounded-lg">
                {page > 1 ? (
                    <Link href={`/address/${addrStr}?page=${page - 1}`} className="text-primary hover:underline">&larr; Previous</Link>
                ) : <span className="text-muted-foreground pointer-events-none opacity-50">&larr; Previous</span>}

                <span className="text-sm font-medium text-muted-foreground">Page {addr.page} of {addr.totalPages}</span>

                {page < (addr.totalPages || 0) ? (
                    <Link href={`/address/${addrStr}?page=${page + 1}`} className="text-primary hover:underline">Next &rarr;</Link>
                ) : <span className="text-muted-foreground pointer-events-none opacity-50">Next &rarr;</span>}
            </div>
        </div>
    );
}
