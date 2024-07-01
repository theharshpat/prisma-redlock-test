import { PrismaClient } from '@prisma/client';
import Redis from 'ioredis';
import Redlock from 'redlock';

const prisma = new PrismaClient();
const redis = new Redis({ host: 'localhost', port: 6379 });
const redlock = new Redlock([redis], {
  retryCount: 4,
  retryDelay: 500,
  retryJitter: 10,
});

const userId = "test-user-id";

const wait = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const formatLocalTime = () => {
  const now = new Date();
  const year = now.getFullYear();
  const month = (now.getMonth() + 1).toString().padStart(2, '0');
  const day = now.getDate().toString().padStart(2, '0');
  const hours = now.getHours();
  const minutes = now.getMinutes().toString().padStart(2, '0');
  const seconds = now.getSeconds().toString().padStart(2, '0');
  const milliseconds = now.getMilliseconds().toString().padStart(3, '0');
  const period = hours >= 12 ? 'PM' : 'AM';
  const adjustedHours = (hours % 12 || 12).toString().padStart(2, '0');
  return `${year}-${month}-${day} ${adjustedHours}:${minutes}:${seconds}.${milliseconds} ${period}`;
};

const logWithTimestamp = (message: string) => {
  const timestamp = formatLocalTime();
  const blue = '\x1b[34m';
  const green = '\x1b[32m';
  const reset = '\x1b[0m';
  console.log(`${blue}[${timestamp}]${reset} ${green}${message}${reset}`);
};

const cleanupDatabase = async () => {
  logWithTimestamp("Cleaning up database...");
  await prisma.job.deleteMany({});
  await prisma.user.deleteMany({});
  logWithTimestamp("Database cleaned up.");
};

const initializeUser = async () => {
  logWithTimestamp("Initializing user...");
  await prisma.user.upsert({
    where: { id: userId },
    update: {},
    create: {
      id: userId,
      name: 'Test User',
      balance: 100,
    },
  });
  logWithTimestamp("User initialized with balance: 100");
};

const createJobWithLock = async (
  userId: string, 
  jobTitle: string, 
  cost: number, 
  useRowLock: boolean = true, 
  processingTime: number = 1000
): Promise<void> => {
  logWithTimestamp(`[${jobTitle}] Starting transaction`);
  return await prisma.$transaction(async (prisma) => {
    if (useRowLock) {
      logWithTimestamp(`[${jobTitle}] Attempting to acquire row lock for user ${userId}...`);
      await prisma.$executeRaw`SELECT * FROM "User" WHERE id = ${userId} FOR UPDATE`;
      logWithTimestamp(`[${jobTitle}] Row lock acquired for user ${userId}.`);
    } else {
      logWithTimestamp(`[${jobTitle}] Skipping row lock.`);
    }

    const user = await prisma.user.findUnique({ where: { id: userId } });
    if (!user) {
      throw new Error('User not found');
    }

    logWithTimestamp(`[${jobTitle}] Current balance: ${user.balance}`);

    if (cost > user.balance) {
      throw new Error('Not enough balance');
    }

    logWithTimestamp(`[${jobTitle}] Processing...`);
    await wait(processingTime);

    await prisma.user.update({
      where: { id: user.id },
      data: { balance: user.balance - cost },
    });

    await prisma.job.create({
      data: {
        title: jobTitle,
        userId: user.id,
      },
    });

    const updatedUser = await prisma.user.findUnique({ where: { id: userId } });
    logWithTimestamp(`[${jobTitle}] Job created and balance updated. New balance: ${updatedUser?.balance}`);
  }, {
    timeout: 5500
  }).then(() => {
    logWithTimestamp(`[${jobTitle}] Transaction completed successfully`);
  }).catch((error) => {
    logWithTimestamp(`[${jobTitle}] Transaction failed: ${error}`);
    throw error;
  });
};

const scenario1 = async () => {
  logWithTimestamp("Scenario 1: Successful Lock and Transaction");
  await cleanupDatabase();
  await initializeUser();

  const resource = `locks:user:${userId}`;

  try {
    const lock = await redlock.acquire([resource], 5000);
    logWithTimestamp("Redlock acquired for Scenario 1");
    try {
      await createJobWithLock(userId, "Job 1", 30);
    } finally {
      await lock.release();
      logWithTimestamp("Redlock released for Scenario 1");
    }
  } catch (error) {
    logWithTimestamp(`Error in Scenario 1: ${error}`);
  }
};

const scenario2 = async () => {
  logWithTimestamp("Scenario 2: Redlock Contention");
  await cleanupDatabase();
  await initializeUser();

  const resource = `locks:user:${userId}`;

  const job1 = async () => {
    try {
      const lock = await redlock.acquire([resource], 3000);
      logWithTimestamp("Redlock acquired for Job 1");
      try {
        await createJobWithLock(userId, "Job 2.1", 20, true, 2000);
      } finally {
        await lock.release();
        logWithTimestamp("Redlock released for Job 1");
      }
    } catch (error) {
      logWithTimestamp(`Error in Job 1: ${error}`);
    }
  };

  const job2 = async () => {
    try {
      const lock = await redlock.acquire([resource], 3000);
      logWithTimestamp("Redlock acquired for Job 2");
      try {
        await createJobWithLock(userId, "Job 2.2", 30, true, 1000);
      } finally {
        await lock.release();
        logWithTimestamp("Redlock released for Job 2");
      }
    } catch (error) {
      logWithTimestamp(`Error in Job 2: ${error}`);
    }
  };

  await Promise.all([job1(), job2()]);
};

const scenario3 = async () => {
  logWithTimestamp("Scenario 3: Prisma Transaction Timeout");
  await cleanupDatabase();
  await initializeUser();

  try {
    await createJobWithLock(userId, "Job 3", 30, true, 5600); // Set processing time longer than transaction timeout
  } catch (error) {
    logWithTimestamp(`Error in Scenario 3 (Expected timeout): ${error}`);
  }
};

const scenario4a = async () => {
  logWithTimestamp("Scenario 4A: Both jobs with row locks");
  await cleanupDatabase();
  await initializeUser();

  const job1 = createJobWithLock(userId, "Job 4A.1", 20, true, 1400);
  const job2 = createJobWithLock(userId, "Job 4A.2", 30, true, 1200);

  await Promise.all([job1, job2]);
};

const scenario4b = async () => {
  logWithTimestamp("Scenario 4B: First job with row lock, second without");
  await cleanupDatabase();
  await initializeUser();

  const job1 = createJobWithLock(userId, "Job 4B.1", 20, true, 1400);
  const job2 = createJobWithLock(userId, "Job 4B.2", 30, false, 1200);

  await Promise.all([job1, job2]);
};

const scenario4c = async () => {
  logWithTimestamp("Scenario 4C: Both jobs without row locks");
  await cleanupDatabase();
  await initializeUser();

  const job1 = createJobWithLock(userId, "Job 4C.1", 20, false, 1400);
  const job2 = createJobWithLock(userId, "Job 4C.2", 30, false, 1200);

  await Promise.all([job1, job2]);
};

const scenario4d = async () => {
  logWithTimestamp("Scenario 4D: Sequential execution with row locks");
  await cleanupDatabase();
  await initializeUser();

  await createJobWithLock(userId, "Job 4D.1", 20, true, 1400);
  await createJobWithLock(userId, "Job 4D.2", 30, true, 1200);
};

const scenario4e = async () => {
  logWithTimestamp("Scenario 4E: Three concurrent jobs with varying lock usage");
  await cleanupDatabase();
  await initializeUser();

  const job1 = createJobWithLock(userId, "Job 4E.1", 20, true, 1000);
  const job2 = createJobWithLock(userId, "Job 4E.2", 23, true, 2000);
  const job3 = createJobWithLock(userId, "Job 4E.3", 10, true, 1000);

  await Promise.all([job1, job2, job3]);
};

const scenario5 = async () => {
  logWithTimestamp("Scenario 5: Optimistic Concurrency Control");
  await cleanupDatabase();
  await initializeUser();

  const updateBalance = async (amount: number) => {
    try {
      await prisma.$transaction(async (prisma) => {
        const user = await prisma.user.findUnique({ where: { id: userId } });
        if (!user) throw new Error('User not found');

        logWithTimestamp(`Updating balance by ${amount}. Current balance: ${user.balance}`);
        await wait(1000); // Simulate some processing time

        const updatedUser = await prisma.user.update({
          where: { id: userId, balance: user.balance }, // Optimistic lock
          data: { balance: user.balance + amount },
        });

        logWithTimestamp(`Balance updated: ${updatedUser.balance}`);
      });
    } catch (error) {
      logWithTimestamp(`Error updating balance: ${error}`);
    }
  };

  await Promise.all([
    updateBalance(10),
    updateBalance(20),
    updateBalance(-5),
  ]);
};

const runScenarios = async () => {
  await scenario1();
  await scenario2();
  await scenario3();
  await scenario4a();
  await scenario4b();
  await scenario4c();
  await scenario4d();
  await scenario4e();
  await scenario5();
};

runScenarios().catch(console.error).finally(() => {
  prisma.$disconnect();
  redis.disconnect();
});