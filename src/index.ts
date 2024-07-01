import { PrismaClient } from '@prisma/client';
import Redis from 'ioredis';
import Redlock from 'redlock';

const prisma = new PrismaClient();
const redis = new Redis({ host: 'localhost', port: 6379 });
const redlock = new Redlock([redis], {
  retryCount: 2,
  retryDelay: 200,
  retryJitter: 50,
});

const userId = "test-user-id";

const wait = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const cleanupDatabase = async () => {
  console.log("Cleaning up database...");
  await prisma.job.deleteMany({});
  await prisma.user.deleteMany({});
  console.log("Database cleaned up.");
};

const initializeUser = async () => {
  console.log("Initializing user...");
  await prisma.user.upsert({
    where: { id: userId },
    update: {},
    create: {
      id: userId,
      name: 'Test User',
      balance: 100,
    },
  });
  console.log("User initialized.");
};

const createJobWithLock = async (userId: string, jobTitle: string, cost: number, useRowLock: boolean = true, processingTime: number = 1000): Promise<void> => {
  return await prisma.$transaction(async (prisma) => {
    if (useRowLock) {
      console.log(`[${jobTitle}] Attempting to lock user ${userId}...`);
      await prisma.$executeRaw`SELECT * FROM "User" WHERE id = ${userId} FOR UPDATE`;
      console.log(`[${jobTitle}] User ${userId} locked.`);
    } else {
      console.log(`[${jobTitle}] Skipping row lock.`);
    }

    const user = await prisma.user.findUnique({ where: { id: userId } });
    if (!user) {
      throw new Error('User not found');
    }

    if (cost > user.balance) {
      throw new Error('Not enough balance');
    }

    console.log(`[${jobTitle}] Processing...`);
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

    console.log(`[${jobTitle}] Job created and balance updated.`);
  }, {
    timeout: 5000
  });
};

const scenario1 = async () => {
  console.log("\nScenario 1: Successful Lock and Transaction");
  await cleanupDatabase();
  await initializeUser();

  const resource = `locks:user:${userId}`;

  try {
    const lock = await redlock.acquire([resource], 5000);
    console.log("Redlock acquired for Scenario 1");
    try {
      await createJobWithLock(userId, "Job 1", 30);
    } finally {
      await lock.release();
      console.log("Redlock released for Scenario 1");
    }
  } catch (error) {
    console.error("Error in Scenario 1:", error);
  }
};

const scenario2 = async () => {
  console.log("\nScenario 2: Redlock Contention");
  await cleanupDatabase();
  await initializeUser();

  const resource = `locks:user:${userId}`;

  const job1 = async () => {
    try {
      const lock = await redlock.acquire([resource], 3000);
      console.log("Redlock acquired for Job 1");
      try {
        await createJobWithLock(userId, "Job 2.1", 20, true, 2000);
      } finally {
        await lock.release();
        console.log("Redlock released for Job 1");
      }
    } catch (error) {
      console.error("Error in Job 1:", error);
    }
  };

  const job2 = async () => {
    try {
      const lock = await redlock.acquire([resource], 3000);
      console.log("Redlock acquired for Job 2");
      try {
        await createJobWithLock(userId, "Job 2.2", 30, true, 1000);
      } finally {
        await lock.release();
        console.log("Redlock released for Job 2");
      }
    } catch (error) {
      console.error("Error in Job 2:", error);
    }
  };

  await Promise.all([job1(), job2()]);
};

const scenario3 = async () => {
  console.log("\nScenario 3: Prisma Transaction Timeout");
  await cleanupDatabase();
  await initializeUser();

  try {
    await createJobWithLock(userId, "Job 3", 30, true, 6000); // Set processing time longer than transaction timeout
  } catch (error) {
    console.error("Error in Scenario 3 (Expected timeout):", error);
  }
};

const scenario4 = async () => {
  console.log("\nScenario 4: Row-Level Lock vs No Lock");
  await cleanupDatabase();
  await initializeUser();

  const job1 = createJobWithLock(userId, "Job 4.1", 20, true, 2000);
  const job2 = createJobWithLock(userId, "Job 4.2", 30, false, 1000);

  await Promise.all([job1, job2]);
};

const scenario5 = async () => {
  console.log("\nScenario 5: Optimistic Concurrency Control");
  await cleanupDatabase();
  await initializeUser();

  const updateBalance = async (amount: number) => {
    try {
      await prisma.$transaction(async (prisma) => {
        const user = await prisma.user.findUnique({ where: { id: userId } });
        if (!user) throw new Error('User not found');

        await wait(1000); // Simulate some processing time

        const updatedUser = await prisma.user.update({
          where: { id: userId, balance: user.balance }, // Optimistic lock
          data: { balance: user.balance + amount },
        });

        console.log(`Balance updated: ${updatedUser.balance}`);
      });
    } catch (error) {
      console.error("Error updating balance:", error);
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
  await scenario4();
  await scenario5();
};

runScenarios().catch(console.error).finally(() => {
  prisma.$disconnect();
  redis.disconnect();
});