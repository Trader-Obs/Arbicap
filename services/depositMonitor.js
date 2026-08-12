/**
 * Deposit Monitor — Company Wallet / Auto-Credit Model
 *
 * Flow:
 *   1. User deposits to the company wallet address
 *   2. User submits their tx hash + amount via the platform
 *   3. Balance is credited to their dashboard IMMEDIATELY
 *   4. A record appears in admin for your financial team's reference
 *   5. Your team invests the actual funds — user balances are internal ledger entries
 *
 * Rules:
 *   - No deposit minimum
 *   - No KYC required to deposit
 *   - No daily deposit limit
 *   - Auto-credited on submission (no admin approval needed for deposits)
 *   - Withdrawals require KYC and admin approval (on hold until launch)
 */
'use strict';
const cron   = require('node-cron');
const { query, getClient } = require('../models/db');
const { redis }     = require('./redis');
const { sendEmail } = require('./email');
const { notifyUser }= require('./notifications');
const { auditLog }  = require('./audit');
const { logger }    = require('./logger');

function startDepositMonitor() {
  // Daily cleanup
  cron.schedule('0 2 * * *', async () => {
    try {
      await query('DELETE FROM sessions WHERE expires_at < NOW()');
      await query("DELETE FROM email_verifications WHERE expires_at < NOW() AND used_at IS NULL");
      logger.info('Daily cleanup completed');
    } catch (err) { logger.error('Cleanup error:', err.message); }
  });

  // Process withdrawal queue every 5 minutes (disabled until WITHDRAWALS_LIVE=true)
  cron.schedule('*/5 * * * *', async () => {
    try { await processWithdrawalQueue(); }
    catch (err) { logger.error('Withdrawal queue error:', err.message); }
  });

  logger.info('Deposit monitor started (auto-credit mode — no approval required)');
}

// ── AUTO-CREDIT DEPOSIT ───────────────────────
// Called immediately when user submits their tx hash.
// No minimum, no KYC, no approval needed.
async function submitDeposit({ userId, symbol, network, txHash, amount }) {
  if (!txHash || !txHash.trim()) throw new Error('Transaction hash is required.');
  if (!amount || parseFloat(amount) <= 0) throw new Error('Amount must be greater than zero.');

  const amt = parseFloat(amount);

  // Prevent duplicate tx hash submissions
  const existing = await query(
    'SELECT id, status FROM transactions WHERE tx_hash = $1',
    [txHash.trim()]
  );
  if (existing.rows.length) {
    if (existing.rows[0].status === 'completed') throw new Error('This transaction has already been credited.');
    throw new Error('This transaction hash has already been submitted. Contact support if you have an issue.');
  }

  const client = await getClient();
  try {
    await client.query('BEGIN');

    // Create completed transaction record immediately
    const txResult = await client.query(`
      INSERT INTO transactions
        (user_id, type, symbol, network, amount, fee, status, tx_hash, note, completed_at)
      VALUES ($1, 'deposit', $2, $3, $4, 0, 'completed', $5,
              'Auto-credited on submission — company wallet deposit', NOW())
      RETURNING id
    `, [userId, symbol, network, amt, txHash.trim()]);

    const txId = txResult.rows[0].id;

    // Credit balance immediately
    await client.query(`
      INSERT INTO wallets (user_id, symbol, total_balance, locked_balance)
      VALUES ($1, $2, $3, 0)
      ON CONFLICT (user_id, symbol)
      DO UPDATE SET total_balance = wallets.total_balance + $3, updated_at = NOW()
    `, [userId, symbol, amt]);

    await client.query('COMMIT');

    // Notify user
    const userRes = await query('SELECT email, first_name FROM users WHERE id = $1', [userId]);
    if (userRes.rows.length) {
      const u = userRes.rows[0];
      await sendEmail({
        to: u.email,
        subject: `Deposit confirmed — ${amt} ${symbol}`,
        template: 'deposit-confirmed',
        data: { name: u.first_name, amount: amt, symbol, confirmations: 1 },
      });
    }

    await notifyUser(userId, 'deposit',
      `${amt} ${symbol} deposit confirmed`,
      { body: 'Your balance has been updated.', link: '/dashboard.html' }
    );
    await auditLog({ userId, action: 'deposit_auto_credited', entityType: 'transaction', entityId: txId });
    logger.info(`Deposit auto-credited: ${txId} — ${amt} ${symbol} for user ${userId}`);
    return { txId, amount: amt, symbol, status: 'completed' };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

// ── WITHDRAWAL PROCESSOR ──────────────────────
// Disabled until WITHDRAWALS_LIVE=true in .env
async function processWithdrawalQueue() {
  const live = process.env.WITHDRAWALS_LIVE === 'true';
  if (!live) return;

  const { broadcastWithdrawal } = require('./custody');

  for (let i = 0; i < 10; i++) {
    const raw = await redis.lpop('withdrawal_queue');
    if (!raw) break;

    let job;
    try { job = JSON.parse(raw); } catch { continue; }

    const client = await getClient();
    try {
      await client.query('BEGIN');
      await client.query("UPDATE transactions SET status='processing' WHERE id=$1", [job.txId]);

      const result = await broadcastWithdrawal({
        txId: job.txId, symbol: job.symbol,
        network: job.network, toAddress: job.address, amount: job.amount,
      });

      await client.query(`
        UPDATE transactions SET status='completed', tx_hash=$1, completed_at=NOW() WHERE id=$2
      `, [result.txHash, job.txId]);

      await client.query(`
        UPDATE wallets
        SET total_balance  = total_balance  - $1,
            locked_balance = GREATEST(0, locked_balance - $1)
        WHERE user_id=$2 AND symbol=$3
      `, [job.amount + (job.fee || 0), job.userId, job.symbol]);

      await client.query('COMMIT');
      await notifyUser(job.userId, 'withdrawal', `${job.amount} ${job.symbol} withdrawal sent`);
      logger.info(`Withdrawal processed: ${job.txId}`);
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      await redis.rpush('withdrawal_queue_failed', raw);
      await query("UPDATE transactions SET status='pending_review', admin_note=$1 WHERE id=$2",
        [err.message, job.txId]);
      logger.error(`Withdrawal failed: ${job.txId} — ${err.message}`);
    } finally {
      client.release();
    }
  }
}

module.exports = { startDepositMonitor, submitDeposit };
