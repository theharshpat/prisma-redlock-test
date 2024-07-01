import { PrismaClient } from '@prisma/client';
import Redis from 'ioredis';
import Redlock from 'redlock';

const prisma = new PrismaClient();
const redis = new Redis({ host: 'localhost', port: 6379 });
const redlock = new Redlock([redis], {
  retryCount: 10, // Retry up to 10 times
  retryDelay: 500, // Wait for 500 ms before retrying
  retryJitter: 50, // Add random jitter of 0-50 ms
});

const userId = "test-user-id";

// Wait function to simulate delays
const wait = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// Clean up the database
const cleanupDatabase = async () => {
  console.log("Cleaning up database...");
  await prisma.job.deleteMany({});
  await prisma.user.deleteMany({});
  console.log("Database cleaned up.");
};

// Initialize the test user with a balance
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

// Function to create a job and deduct balance with locking
const createJobWithLock = async (userId: string, jobTitle: string, cost: number): Promise<void> => {
  return await prisma.$transaction(async (prisma) => {
    console.log(`Attempting to lock user ${userId} for ${jobTitle}...`);
    // Lock the user row
    await prisma.$executeRaw`SELECT * FROM "User" WHERE id = ${userId} FOR UPDATE`;
    console.log(`User ${userId} locked for ${jobTitle}.`);

    const user = await prisma.user.findUnique({ where: { id: userId } });
    if (!user) {
      throw new Error('User not found');
    }

    if (cost > user.balance) {
      throw new Error('Not enough balance');
    }

    console.log(`Processing ${jobTitle}...`);
    await wait(5000); // Simulate long processing time

    // Deduct balance and create the job
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

    console.log(`Job ${jobTitle} created and balance updated.`);
  });
};

// Test function
const testLocking = async () => {
  const resource = `locks:user:${userId}`;

  try {
    // Clean up and initialize user
    await cleanupDatabase();
    await initializeUser();

    // Simulate concurrent job creation requests
    const job1 = redlock.acquire([resource], 15000)
      .then(async (lock) => {
        console.log("Lock acquired for job 1");
        try {
          await createJobWithLock(userId, "Job 1", 30);
          console.log("Job 1 created");
        } finally {
          await lock.release();
          console.log("Lock released for job 1");
        }
      });

    await wait(2000); // Wait 2 seconds before starting job 2

    const job2 = redlock.acquire([resource], 15000)
      .then(async (lock) => {
        console.log("Lock acquired for job 2");
        try {
          await createJobWithLock(userId, "Job 2", 30);
          console.log("Job 2 created");
        } finally {
          await lock.release();
          console.log("Lock released for job 2");
        }
      });

    await Promise.all([job1, job2]);

  } catch (error) {
    console.error("Error in testLocking:", error);
  }
};

testLocking().catch(console.error).finally(() => {
  prisma.$disconnect();
  redis.disconnect();
});
