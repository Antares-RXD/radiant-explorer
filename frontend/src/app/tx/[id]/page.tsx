'use client'

import { useQuery } from '@tanstack/react-query';
import { api } from '@/lib/api';
import { Card } from '@/components/ui/Card';
import { TxIdDisplay } from '@/components/TxIdDisplay';
import Link from 'next/link';
import { useParams } from 'next/navigation';

export default function TxPage() {
    const { id } = useParams();
    const txid = id as string;

    const { data: tx, isLoading, error } = useQuery({
        queryKey: ['tx', txid],
        queryFn: () => api.getTx(txid),
        enabled: !!txid
    });

    if (isLoading) return <div className="text-center p-8 text-muted-foreground">Loading Transaction...</div>;
    if (error) return <div className="text-center p-8 text-destructive">Error loading transaction</div>;
    if (!tx) return <div className="text-center p-8 text-destructive">Transaction not found</div>;

    const totalOutput = tx.vout.reduce((sum, vout) => sum + (parseFloat(vout.value || "0") || 0), 0);

    return (
        <div className="flex flex-col gap-6">
            <div>
                <h1 className="text-3xl font-bold mb-2">Transaction Details</h1>
                <div className="text-muted-foreground font-mono bg-muted/30 p-2 rounded inline-block text-sm lg:text-base max-w-full">
                    <TxIdDisplay txid={tx.txid} className="text-sm lg:text-base" forceFull />
                </div>
            </div>

            <Card title="Summary">
                <div className="p-6 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                    <div className="flex flex-col gap-1">
                        <span className="text-sm font-medium text-muted-foreground uppercase tracking-wider">Block Height</span>
                        <Link href={`/block/${tx.height}`} className="text-primary font-bold hover:underline">{tx.height}</Link>
                    </div>
                    <div className="flex flex-col gap-1">
                        <span className="text-sm font-medium text-muted-foreground uppercase tracking-wider">Time</span>
                        <span className="font-mono">{new Date(tx.blocktime * 1000).toLocaleString()}</span>
                    </div>
                    <div className="flex flex-col gap-1">
                        <span className="text-sm font-medium text-muted-foreground uppercase tracking-wider">Total Output</span>
                        <span className="font-mono font-bold text-green-600 dark:text-green-400">{totalOutput.toFixed(8)} <span className="text-muted-foreground text-sm font-normal">RXD</span></span>
                    </div>
                    {/* Fee calculation requires previous outputs, not available in raw tx */}
                    <div className="flex flex-col gap-1">
                        <span className="text-sm font-medium text-muted-foreground uppercase tracking-wider">Size</span>
                        <span className="font-mono">{tx.size} bytes</span>
                    </div>
                </div>
            </Card>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
                <Card title="Inputs">
                    <ul className="p-6 space-y-4 max-h-[500px] overflow-y-auto custom-scrollbar">
                        {tx.vin.map((vin, idx) => (
                            <li key={idx} className="flex justify-between items-center p-3 bg-muted/20 rounded-lg border border-transparent hover:border-border transition-colors">
                                <div className="flex-1 min-w-0 mr-4">
                                    {vin.coinbase ? (
                                        <span className="text-yellow-600 dark:text-yellow-400 font-medium text-sm">Coinbase (Newly Generated Coins)</span>
                                    ) : (
                                        <Link href={`/address/${vin.addresses?.[0]}`} className="text-primary font-mono text-sm hover:underline block truncate">
                                            {vin.addresses?.[0] || 'Unknown'}
                                        </Link>
                                    )}
                                </div>
                                <div className="font-mono text-sm whitespace-nowrap">
                                    {vin.value ? parseFloat(vin.value).toFixed(8) : ''} RXD
                                </div>
                            </li>
                        ))}
                    </ul>
                </Card>
                <Card title="Outputs">
                    <ul className="p-6 space-y-4 max-h-[500px] overflow-y-auto custom-scrollbar">
                        {tx.vout.filter(vout => vout.scriptPubKey?.addresses?.[0]).map((vout, idx) => (
                            <li key={idx} className="flex justify-between items-center p-3 bg-muted/20 rounded-lg border border-transparent hover:border-border transition-colors">
                                <div className="flex-1 min-w-0 mr-4">
                                    <Link href={`/address/${vout.scriptPubKey!.addresses![0]}`} className="text-primary font-mono text-sm lg:text-base hover:underline block truncate">
                                        {vout.scriptPubKey!.addresses![0]}
                                    </Link>
                                </div>
                                <div className="font-mono text-sm lg:text-base whitespace-nowrap font-bold text-green-600 dark:text-green-400">
                                    {parseFloat(vout.value).toFixed(8)} RXD
                                </div>
                            </li>
                        ))}
                    </ul>
                </Card>
            </div>
        </div>
    );
}
