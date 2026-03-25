import { Router } from 'express';
import { ingestData, sendCommand } from '../controllers/loggerController.js';

const router = Router();
router.post('/log', ingestData);
router.post('/command', sendCommand);
export default router;