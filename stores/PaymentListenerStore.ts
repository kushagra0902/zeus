import { action, observable, runInAction } from 'mobx';
import {
    NativeEventEmitter,
    NativeModules,
    EmitterSubscription
} from 'react-native';

import SettingsStore from './SettingsStore';
import BalanceStore from './BalanceStore';
import ChannelsStore from './ChannelsStore';
import ActivityStore from './ActivityStore';

import lndMobile from '../lndmobile/LndMobileInjection';
import { decodeSubscribeTransactionsResult } from '../lndmobile/onchain';
import {
    checkLndStreamErrorResponse,
    LndMobileEventEmitter
} from '../utils/LndMobileUtils';
import BackendUtils from '../utils/BackendUtils';
import Base64Utils from '../utils/Base64Utils';

export default class PaymentListenerStore {
    settingsStore: SettingsStore;
    balanceStore: BalanceStore;
    channelsStore: ChannelsStore;
    activityStore: ActivityStore;

    // event listeners (embedded-lnd, LNC)
    private invoiceListener: EmitterSubscription | null = null;
    private txListener: EmitterSubscription | null = null;
    private ldkUnsubscribe: (() => void) | null = null;

    // polling intervals (REST, CLN-REST, LndHub)
    private invoicePollInterval: ReturnType<typeof setInterval> | null = null;

    private lastSettleIndex: number = 0;

    @observable public isSubscribed: boolean = false;

    @observable public lastSettledInvoice: any = null;

    @observable public lastOnChainTx: any = null;

    constructor(
        settingsStore: SettingsStore,
        balanceStore: BalanceStore,
        channelsStore: ChannelsStore,
        activityStore: ActivityStore
    ) {
        this.settingsStore = settingsStore;
        this.balanceStore = balanceStore;
        this.channelsStore = channelsStore;
        this.activityStore = activityStore;
    }

    @action
    public reset = () => {
        this.stopListening();
        this.lastSettledInvoice = null;
        this.lastOnChainTx = null;
        this.lastSettleIndex = 0;
    };

    @action
    public startListening = async () => {
        if (this.isSubscribed) return;
        this.isSubscribed = true;

        const { implementation } = this.settingsStore;

        switch (implementation) {
            case 'embedded-lnd':
                await this.startEmbeddedLndListeners();
                break;
            case 'lightning-node-connect':
                this.startLncListeners();
                break;
            case 'lnd':
                this.startPolling(5000, 'lnd');
                break;
            case 'cln-rest':
                this.startPolling(5000, 'cln-rest');
                break;
            case 'lndhub':
                this.startPolling(10000, 'lndhub');
                break;
            case 'ldk-node':
                this.startLdkNodeListeners();
                break;
            default:
                break;
        }
    };

    @action
    public stopListening = () => {
        if (this.invoiceListener) {
            this.invoiceListener.remove();
            this.invoiceListener = null;
        }
        if (this.txListener) {
            this.txListener.remove();
            this.txListener = null;
        }
        if (this.invoicePollInterval) {
            clearInterval(this.invoicePollInterval);
            this.invoicePollInterval = null;
        }
        if (this.ldkUnsubscribe) {
            this.ldkUnsubscribe();
            this.ldkUnsubscribe = null;
        }
        this.isSubscribed = false;
    };

    private startEmbeddedLndListeners = async () => {
        // Invoice listener
        this.invoiceListener = LndMobileEventEmitter.addListener(
            'SubscribeInvoices',
            (e: any) => {
                try {
                    const error = checkLndStreamErrorResponse(
                        'SubscribeInvoices',
                        e
                    );
                    if (error === 'EOF') {
                        return;
                    } else if (error) {
                        console.error(
                            '[PaymentListenerStore] SubscribeInvoices error',
                            error
                        );
                        return;
                    }

                    const invoice = lndMobile.wallet.decodeInvoiceResult(
                        e.data
                    );

                    if (invoice.settled) {
                        runInAction(() => {
                            this.lastSettledInvoice = {
                                r_hash: Base64Utils.bytesToHex(
                                    // @ts-ignore:next-line
                                    invoice.r_hash
                                ),
                                amt_paid_sat: Number(invoice.amt_paid_sat),
                                payment_request: invoice.payment_request,
                                r_preimage: Base64Utils.bytesToHex(
                                    // @ts-ignore:next-line
                                    invoice.r_preimage
                                )
                            };
                        });
                        this.refreshBalancesAndChannels();
                    }
                } catch (error) {
                    console.error(
                        '[PaymentListenerStore] SubscribeInvoices decode error',
                        error
                    );
                }
            }
        );

        this.txListener = LndMobileEventEmitter.addListener(
            'SubscribeTransactions',
            (e: any) => {
                try {
                    const error = checkLndStreamErrorResponse(
                        'SubscribeTransactions',
                        e
                    );
                    if (error === 'EOF') {
                        return;
                    } else if (error) {
                        console.error(
                            '[PaymentListenerStore] SubscribeTransactions error',
                            error
                        );
                        return;
                    }

                    const transaction = decodeSubscribeTransactionsResult(
                        e.data
                    );

                    if (Number(transaction.amount) > 0) {
                        runInAction(() => {
                            this.lastOnChainTx = {
                                tx_hash: transaction.tx_hash,
                                amount: transaction.amount,
                                dest_addresses: transaction.dest_addresses,
                                num_confirmations: transaction.num_confirmations
                            };
                        });
                        this.refreshBalancesAndChannels();
                    }
                } catch (error) {
                    console.error(
                        '[PaymentListenerStore] SubscribeTransactions decode error',
                        error
                    );
                }
            }
        );

        await lndMobile.wallet.subscribeInvoices();
        await lndMobile.onchain.subscribeTransactions();
    };

    private startLncListeners = () => {
        const { LncModule } = NativeModules;
        const eventEmitter = new NativeEventEmitter(LncModule);

        // Invoice stream
        const invoiceEventName = BackendUtils.subscribeInvoices();
        if (invoiceEventName) {
            this.invoiceListener = eventEmitter.addListener(
                invoiceEventName,
                (event: any) => {
                    if (!event.result) return;
                    if (
                        typeof event.result === 'string' &&
                        event.result.includes('rpc error: code = Canceled')
                    ) {
                        return;
                    }
                    try {
                        const result = JSON.parse(event.result);
                        if (result === 'EOF') return;
                        if (result.settled) {
                            runInAction(() => {
                                this.lastSettledInvoice = {
                                    r_hash: result.r_hash,
                                    amt_paid_sat: Number(result.amt_paid_sat),
                                    payment_request: result.payment_request,
                                    r_preimage: result.r_preimage
                                };
                            });
                            this.refreshBalancesAndChannels();
                        }
                    } catch (error) {
                        console.error(
                            '[PaymentListenerStore] LNC invoice error',
                            error
                        );
                    }
                }
            );
        }

        const txEventName = BackendUtils.subscribeTransactions();
        if (txEventName) {
            this.txListener = eventEmitter.addListener(
                txEventName,
                (event: any) => {
                    if (!event.result) return;
                    try {
                        const result = JSON.parse(event.result);
                        if (result === 'EOF') return;
                        if (Number(result.amount) > 0) {
                            runInAction(() => {
                                this.lastOnChainTx = {
                                    tx_hash: result.tx_hash,
                                    amount: result.amount,
                                    dest_addresses: result.dest_addresses,
                                    num_confirmations: result.num_confirmations
                                };
                            });
                            this.refreshBalancesAndChannels();
                        }
                    } catch (error) {
                        console.error(
                            '[PaymentListenerStore] LNC tx error',
                            error
                        );
                    }
                }
            );
        }
    };

    private startLdkNodeListeners = () => {
        const ldkBackend = BackendUtils.ldkNode;

        this.ldkUnsubscribe = ldkBackend.subscribeToEvents((event: any) => {
            try {
                if (event.type === 'paymentReceived') {
                    const amountSat = Math.floor(event.amountMsat / 1000);

                    runInAction(() => {
                        this.lastSettledInvoice = {
                            r_hash: event.paymentHash,
                            amt_paid_sat: amountSat,
                            payment_request: event.paymentHash, // fallback as LDK events often just have hash
                            r_preimage: null
                        };
                    });
                    this.refreshBalancesAndChannels();
                }
            } catch (error) {
                console.error(
                    '[PaymentListenerStore] LDK Node invoice error',
                    error
                );
            }
        });
    };

    private startPolling = (intervalMs: number, backend: string) => {
        this.invoicePollInterval = setInterval(async () => {
            try {
                const response = await BackendUtils.getInvoices({ limit: 5 });
                const invoices = response?.invoices;
                if (!invoices || invoices.length === 0) return;

                for (const inv of invoices) {
                    const settleIndex = Number(inv.settle_index || 0);
                    const isPaid = this.isInvoicePaid(inv, backend);

                    if (isPaid && settleIndex > this.lastSettleIndex) {
                        this.lastSettleIndex = settleIndex;

                        const amtPaid = this.getAmountPaid(inv, backend);

                        runInAction(() => {
                            this.lastSettledInvoice = {
                                r_hash: inv.r_hash || inv.payment_hash,
                                amt_paid_sat: amtPaid,
                                payment_request:
                                    inv.payment_request || inv.bolt11,
                                r_preimage: inv.r_preimage
                            };
                        });
                        this.refreshBalancesAndChannels();
                        break; // process one new settlement per poll
                    }
                }
            } catch (error) {
                // silently continue polling on errors
                console.warn('[PaymentListenerStore] poll error', error);
            }
        }, intervalMs);
    };

    private isInvoicePaid = (inv: any, backend: string): boolean => {
        switch (backend) {
            case 'lnd':
                return (
                    inv.settled === true ||
                    (Number(inv.amt_paid_sat) > 0 && inv.state === 'SETTLED')
                );
            case 'cln-rest':
                return Number(inv.amount_received_msat) > 0;
            case 'lndhub':
                return inv.ispaid === true;
            default:
                return false;
        }
    };

    private getAmountPaid = (inv: any, backend: string): number => {
        switch (backend) {
            case 'lnd':
                return Number(inv.amt_paid_sat || 0);
            case 'cln-rest':
                return Math.floor(Number(inv.amount_received_msat || 0) / 1000);
            case 'lndhub':
                return Number(inv.amt || 0);
            default:
                return 0;
        }
    };

    private refreshBalancesAndChannels = () => {
        setTimeout(() => {
            if (this.balanceStore?.getCombinedBalance) {
                this.balanceStore.getCombinedBalance();
            }
            if (this.channelsStore?.getChannels) {
                this.channelsStore.getChannels();
            }
            if (this.activityStore?.updateInvoices) {
                this.activityStore.updateInvoices(
                    this.settingsStore?.settings?.locale
                );
            }
            if (this.activityStore?.updateTransactions) {
                this.activityStore.updateTransactions(
                    this.settingsStore?.settings?.locale
                );
            }
        }, 2000); // added delay to ensure LND has fully resolved/cleared the HTLC from its internal channel state.
    };
}
