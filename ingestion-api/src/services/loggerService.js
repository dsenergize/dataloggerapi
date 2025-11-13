import { loggerQueue, debugQueue } from '../../../shared/config/bullmq.js';
// --- DEFINE YOUR CLEANUP OPTIONS ---
// Define these options once at the top of the file
const jobOptions = {
  removeOnComplete: {
    count: 10, // Keep the last 1000 completed jobs
    age: 60 * 60 * 24, // Keep jobs for a max of 24 hours
  },
  removeOnFail: {
    count: 1000, // Keep the last 5000 failed jobs
  },
};
function parseMessages(raw) {
  const trimmedRaw = raw.trim();
  if (trimmedRaw.startsWith('[')) return JSON.parse(trimmedRaw);
  if (trimmedRaw.includes('}{')) return trimmedRaw.replace(/}\s*{/g, '}\n{').split('\n').map(str => JSON.parse(str));
  return [JSON.parse(trimmedRaw)];
}

async function processPacket(msg) {
  // **MODIFIED**: This line correctly finds the core data whether it's nested or flat.
  const packet = msg.data || msg;

  const { imei, msg: msgType, dtm, time, modbus } = packet;
  const sid = modbus?.[0]?.sid;

  if (msgType === 'login' || msgType === 'sys') {
    await debugQueue.add('system-packets', { packet: msg } ,jobOptions);
    return { success: true, type: 'system' };
  }

  if (msgType === 'log' && !sid) {
    return { success: true, type: 'initialization', skipped: true };
  }

  if (!imei || !sid || !(dtm || time)) {
    return { success: false, error: 'Missing required fields (imei, sid, or dtm)', type: 'invalid' };
  }

  try {
    // We queue the ORIGINAL, UNMODIFIED message. The worker will transform it.
    await loggerQueue.add(
      'logger-packets',
      { packet: msg },
      { jobId: `${imei}_${sid}_${dtm || time}`, jobOptions }
      
    );
    return { success: true, type: 'data' };
  } catch (err) {
    console.error('Error adding job to queue:', err);
    return { success: false, error: 'Internal error while queuing data', type: 'error' };
  }
}

export async function handleIngestion(rawBody) {
  try {
    const messages = parseMessages(rawBody);
    return Promise.all(messages.map(processPacket));
  } catch (error) {
    console.error('Failed to parse incoming message body:', rawBody);
    throw error;
  }
}
