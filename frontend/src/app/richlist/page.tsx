'use client'

import { useQuery } from '@tanstack/react-query';
import { api } from '@/lib/api';
import { Card } from '@/components/ui/Card';
import Link from 'next/link';

export default function RichListPage() {
    const { data: richlist, isLoading, error } = useQuery({
        queryKey: ['richlist'],
        queryFn: () => api.getRichList(100)
    });

    if (isLoading) return <div className="p-8 text-center text-muted-foreground">Loading Rich List...</div>;
    if (error) return <div className="p-8 text-center text-destructive">Error loading rich list</div>;

    return (
        <div className="flex flex-col gap-6">
            <h1 className="text-3xl font-bold">Top 100 Rich List</h1>

            <Card title="Wealth Distribution">
                <div className="overflow-x-auto hidden lg:block">
                    <table className="w-full">
                        <thead>
                            <tr className="border-b border-border bg-muted/30">
                                <th className="text-left p-3 text-muted-foreground font-semibold text-sm lg:text-base">Rank</th>
                                <th className="text-left p-3 text-muted-foreground font-semibold text-sm lg:text-base">Address</th>
                                <th className="text-right p-3 text-muted-foreground font-semibold text-sm lg:text-base">Balance (RXD)</th>
                            </tr>
                        </thead>
                        <tbody>
                            {richlist?.map((item, index) => (
                                <tr key={item.address} className="border-b border-border last:border-0 hover:bg-muted/20 transition-colors">
                                    <td className="p-3 text-center text-muted-foreground w-16">{index + 1}</td>
                                    <td className="p-3">
                                        <Link href={`/address/${item.address}`} className="text-link hover:underline font-mono">
                                            {item.address}
                                        </Link>
                                    </td>
                                    <td className="p-3 text-right font-mono font-semibold">
                                        {parseFloat(item.balance).toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 3 })} RXD
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
                <div className="flex flex-col lg:hidden">
                    {richlist?.map((item, index) => (
                        <div key={item.address} className="grid grid-cols-[64px_1fr] gap-3 p-3 border-b border-border last:border-0 hover:bg-muted/20 transition-colors">
                            <div className="flex items-center justify-center text-muted-foreground">
                                {index + 1}
                            </div>
                            <div className="flex flex-col items-center justify-center min-w-0">
                                <Link href={`/address/${item.address}`} className="text-link hover:underline font-mono truncate max-w-full">
                                    {item.address}
                                </Link>
                                <span className="font-mono font-semibold">
                                    {parseFloat(item.balance).toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 3 })} RXD
                                </span>
                            </div>
                        </div>
                    ))}
                </div>
            </Card>
        </div>
    );
}
