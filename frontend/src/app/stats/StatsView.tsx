'use client';

import {
    BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
    AreaChart, Area, LineChart, Line
} from 'recharts';
import { Card } from "@/components/ui/Card";

interface DailyStats {
    date: string;
    tx_count: number;
    total_output: number;
    sum_difficulty: number;
    block_count: number;
    new_assets_count: number;
    active_address_count: number;
    sum_block_size: number;
    new_supply: number;
}

interface StatsViewProps {
    data: DailyStats[];
}

export default function StatsView({ data }: StatsViewProps) {
    // Calculate cumulative and averages
    let cumulativeSupply = 0; // Starting supply? Or assume 0.
    // Ideally fetch initial supply but starting from 0 is OK for trend or if backfilled.
    let cumulativeAssets = 0;

    // Sort by date ASC first for cumulative calc
    const sortedData = [...data].sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());

    const chartData = sortedData.map(d => {
        cumulativeSupply += Number(d.new_supply || 0);
        cumulativeAssets += Number(d.new_assets_count || 0);
        return {
            ...d,
            date: new Date(d.date).toLocaleDateString(),
            avg_difficulty: d.block_count > 0 ? d.sum_difficulty / d.block_count : 0,
            hashrate: d.block_count > 0 ? (d.sum_difficulty / d.block_count) * Math.pow(2, 32) / 60 / 1e12 : 0, // TH/s
            avg_block_size: d.block_count > 0 ? (d.sum_block_size || 0) / d.block_count : 0,
            avg_tx_value: d.tx_count > 0 ? d.total_output / d.tx_count : 0,
            supply: cumulativeSupply,
            total_assets: cumulativeAssets
        };
    }); // No reverse, we want ASC for charts usually? 

    return (
        <div className="space-y-8">
            <h1 className="text-3xl font-bold mb-6">Network Statistics</h1>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
                {/* 1. Transaction Count */}
                <Card title="Daily Transactions">
                    <div className="w-full pt-4 pr-4 pb-4" style={{ height: 300 }}>
                        <ResponsiveContainer width="100%" height="100%">
                            <BarChart data={chartData} margin={{ top: 10, right: 10, left: 20, bottom: 0 }}>
                                <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.2} />
                                <XAxis dataKey="date" fontSize={12} tickMargin={10} minTickGap={30} />
                                <YAxis />
                                <Tooltip
                                    contentStyle={{ backgroundColor: 'var(--background)', borderColor: 'var(--border)' }}
                                    itemStyle={{ color: 'var(--foreground)' }}
                                />
                                <Bar dataKey="tx_count" name="Transactions" fill="#3b82f6" radius={[4, 4, 0, 0]} />
                            </BarChart>
                        </ResponsiveContainer>
                    </div>
                </Card>

                {/* 2. Volume */}
                <Card title="Daily Volume (RXD)">
                    <div className="w-full pt-4 pr-4 pb-4" style={{ height: 300 }}>
                        <ResponsiveContainer width="100%" height="100%">
                            <AreaChart data={chartData} margin={{ top: 10, right: 10, left: 30, bottom: 0 }}>
                                <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.2} />
                                <XAxis dataKey="date" fontSize={12} tickMargin={10} minTickGap={30} />
                                <YAxis
                                    tickFormatter={(value: any) => `${(value / 1_000_000).toFixed(1)}M`}
                                />
                                <Tooltip
                                    contentStyle={{ backgroundColor: 'var(--background)', borderColor: 'var(--border)' }}
                                    itemStyle={{ color: 'var(--foreground)' }}
                                    formatter={(value: any) => value ? `${(value / 1_000_000).toLocaleString('en-US', { maximumFractionDigits: 2 })} M` : '0'}
                                />
                                <Area type="monotone" dataKey="total_output" name="Volume (RXD)" stroke="#10b981" fill="#10b981" fillOpacity={0.2} />
                            </AreaChart>
                        </ResponsiveContainer>
                    </div>
                </Card>

                {/* 3. Difficulty */}
                <Card title="Average Difficulty">
                    <div className="w-full pt-4 pr-4 pb-4" style={{ height: 300 }}>
                        <ResponsiveContainer width="100%" height="100%">
                            <LineChart data={chartData} margin={{ top: 10, right: 10, left: 20, bottom: 0 }}>
                                <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.2} />
                                <XAxis dataKey="date" fontSize={12} tickMargin={10} minTickGap={30} />
                                <YAxis domain={['auto', 'auto']} tickFormatter={(value: any) => value > 1000 ? `${(value / 1000).toFixed(1)}k` : value.toFixed(0)} />
                                <Tooltip
                                    contentStyle={{ backgroundColor: 'var(--background)', borderColor: 'var(--border)' }}
                                    itemStyle={{ color: 'var(--foreground)' }}
                                    formatter={(value: any) => value ? value.toFixed(2) : '0'}
                                />
                                <Line type="monotone" dataKey="avg_difficulty" name="Difficulty" stroke="#f59e0b" dot={false} strokeWidth={2} />
                            </LineChart>
                        </ResponsiveContainer>
                    </div>
                </Card>

                {/* 4. Hashrate */}
                <Card title="Network Hashrate (TH/s)">
                    <div className="w-full pt-4 pr-4 pb-4" style={{ height: 300 }}>
                        <ResponsiveContainer width="100%" height="100%">
                            <AreaChart data={chartData} margin={{ top: 10, right: 10, left: 20, bottom: 0 }}>
                                <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.2} />
                                <XAxis dataKey="date" fontSize={12} tickMargin={10} minTickGap={30} />
                                <YAxis />
                                <Tooltip
                                    contentStyle={{ backgroundColor: 'var(--background)', borderColor: 'var(--border)' }}
                                    itemStyle={{ color: 'var(--foreground)' }}
                                    formatter={(value: any) => value ? `${value.toFixed(2)} TH/s` : '0'}
                                />
                                <Area type="monotone" dataKey="hashrate" name="Hashrate" stroke="#ec4899" fill="#ec4899" fillOpacity={0.2} />
                            </AreaChart>
                        </ResponsiveContainer>
                    </div>
                </Card>

                {/* 5. Active Addresses */}
                <Card title="Daily Active Addresses">
                    <div className="w-full pt-4 pr-4 pb-4" style={{ height: 300 }}>
                        <ResponsiveContainer width="100%" height="100%">
                            <LineChart data={chartData} margin={{ top: 10, right: 10, left: 20, bottom: 0 }}>
                                <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.2} />
                                <XAxis dataKey="date" fontSize={12} tickMargin={10} minTickGap={30} />
                                <YAxis />
                                <Tooltip
                                    contentStyle={{ backgroundColor: 'var(--background)', borderColor: 'var(--border)' }}
                                    itemStyle={{ color: 'var(--foreground)' }}
                                />
                                <Line type="monotone" dataKey="active_address_count" name="Active Addrs" stroke="#6366f1" dot={false} strokeWidth={2} />
                            </LineChart>
                        </ResponsiveContainer>
                    </div>
                </Card>

                {/* 6. Avg Block Size */}
                <Card title="Average Block Size (Bytes)">
                    <div className="w-full pt-4 pr-4 pb-4" style={{ height: 300 }}>
                        <ResponsiveContainer width="100%" height="100%">
                            <LineChart data={chartData} margin={{ top: 10, right: 10, left: 20, bottom: 0 }}>
                                <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.2} />
                                <XAxis dataKey="date" fontSize={12} tickMargin={10} minTickGap={30} />
                                <YAxis />
                                <Tooltip
                                    contentStyle={{ backgroundColor: 'var(--background)', borderColor: 'var(--border)' }}
                                    itemStyle={{ color: 'var(--foreground)' }}
                                    formatter={(value: any) => value ? Math.round(value).toLocaleString('en-US') : '0'}
                                />
                                <Line type="monotone" dataKey="avg_block_size" name="Avg Size" stroke="#14b8a6" dot={false} strokeWidth={2} />
                            </LineChart>
                        </ResponsiveContainer>
                    </div>
                </Card>

                {/* 7. Avg Tx Value */}
                <Card title="Avg Transaction Value (RXD)">
                    <div className="w-full pt-4 pr-4 pb-4" style={{ height: 300 }}>
                        <ResponsiveContainer width="100%" height="100%">
                            <AreaChart data={chartData} margin={{ top: 10, right: 10, left: 20, bottom: 0 }}>
                                <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.2} />
                                <XAxis dataKey="date" fontSize={12} tickMargin={10} minTickGap={30} />
                                <YAxis tickFormatter={(value: any) => `${(value / 1_000_000).toFixed(2)}M`} />
                                <Tooltip
                                    contentStyle={{ backgroundColor: 'var(--background)', borderColor: 'var(--border)' }}
                                    itemStyle={{ color: 'var(--foreground)' }}
                                    formatter={(value: any) => value ? value.toFixed(2) : '0'}
                                />
                                <Area type="monotone" dataKey="avg_tx_value" name="Avg Value" stroke="#8b5cf6" fill="#8b5cf6" fillOpacity={0.2} />
                            </AreaChart>
                        </ResponsiveContainer>
                    </div>
                </Card>

                {/* 8. Circulating Supply */}
                <Card title="Circulating Supply (Estimated)">
                    <div className="w-full pt-4 pr-4 pb-4" style={{ height: 300 }}>
                        <ResponsiveContainer width="100%" height="100%">
                            <AreaChart data={chartData} margin={{ top: 10, right: 10, left: 30, bottom: 0 }}>
                                <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.2} />
                                <XAxis dataKey="date" fontSize={12} tickMargin={10} minTickGap={30} />
                                <YAxis
                                    tickFormatter={(value: any) => `${(value / 1_000_000).toFixed(0)}M`}
                                    domain={['auto', 'auto']}
                                />
                                <Tooltip
                                    contentStyle={{ backgroundColor: 'var(--background)', borderColor: 'var(--border)' }}
                                    itemStyle={{ color: 'var(--foreground)' }}
                                    formatter={(value: any) => value ? `${(value / 1_000_000).toLocaleString('en-US', { maximumFractionDigits: 2 })} M` : '0'}
                                />
                                <Area type="monotone" dataKey="supply" name="Supply" stroke="#eab308" fill="#eab308" fillOpacity={0.1} />
                            </AreaChart>
                        </ResponsiveContainer>
                    </div>
                </Card>


                {/* 9. Total Assets */}
                <Card title="Total Assets Created">
                    <div className="w-full pt-4 pr-4 pb-4" style={{ height: 300 }}>
                        <ResponsiveContainer width="100%" height="100%">
                            <AreaChart data={chartData} margin={{ top: 10, right: 10, left: 20, bottom: 0 }}>
                                <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.2} />
                                <XAxis dataKey="date" fontSize={12} tickMargin={10} minTickGap={30} />
                                <YAxis allowDecimals={false} />
                                <Tooltip
                                    contentStyle={{ backgroundColor: 'var(--background)', borderColor: 'var(--border)' }}
                                    itemStyle={{ color: 'var(--foreground)' }}
                                />
                                <Area type="monotone" dataKey="total_assets" name="Total Assets" stroke="#f43f5e" fill="#f43f5e" fillOpacity={0.1} />
                            </AreaChart>
                        </ResponsiveContainer>
                    </div>
                </Card>

                {/* 10. New Assets (Daily) */}
                <Card title="New Assets (Daily)">
                    <div className="w-full pt-4 pr-4 pb-4" style={{ height: 300 }}>
                        <ResponsiveContainer width="100%" height="100%">
                            <BarChart data={chartData} margin={{ top: 10, right: 10, left: 20, bottom: 0 }}>
                                <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.2} />
                                <XAxis dataKey="date" fontSize={12} tickMargin={10} minTickGap={30} />
                                <YAxis allowDecimals={false} />
                                <Tooltip
                                    contentStyle={{ backgroundColor: 'var(--background)', borderColor: 'var(--border)' }}
                                    itemStyle={{ color: 'var(--foreground)' }}
                                />
                                <Bar dataKey="new_assets_count" name="New Assets" fill="#8b5cf6" radius={[4, 4, 0, 0]} />
                            </BarChart>
                        </ResponsiveContainer>
                    </div>
                </Card>
            </div>
        </div>
    );
}
