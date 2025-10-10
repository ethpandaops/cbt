import type { Meta, StoryObj } from '@storybook/react-vite';
import { SQLCodeBlock } from '@/components/SQLCodeBlock';

const meta = {
  title: 'Components/SQLCodeBlock',
  component: SQLCodeBlock,
  parameters: {
    layout: 'padded',
  },
  tags: ['autodocs'],
  argTypes: {
    title: {
      control: 'text',
      description: 'Title to display above the code block',
    },
    sql: {
      control: 'text',
      description: 'SQL query to display',
    },
  },
  decorators: [
    Story => (
      <div className="bg-slate-950 p-8">
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof SQLCodeBlock>;

export default meta;
type Story = StoryObj<typeof meta>;

export const SimpleQuery: Story = {
  args: {
    title: 'Transformation Query',
    sql: 'SELECT * FROM users WHERE active = true;',
  },
};

export const ComplexQuery: Story = {
  args: {
    title: 'Complex Transformation',
    sql: `SELECT
  u.id,
  u.name,
  u.email,
  COUNT(o.id) as order_count,
  SUM(o.total) as total_spent
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE u.created_at >= '2024-01-01'
  AND u.active = true
GROUP BY u.id, u.name, u.email
HAVING COUNT(o.id) > 5
ORDER BY total_spent DESC
LIMIT 100;`,
  },
};

export const CTEQuery: Story = {
  args: {
    title: 'CTE Transformation Query',
    sql: `WITH active_users AS (
  SELECT id, name, email
  FROM users
  WHERE active = true
    AND last_login > NOW() - INTERVAL '30 days'
),
user_orders AS (
  SELECT
    user_id,
    COUNT(*) as order_count,
    SUM(total) as total_amount
  FROM orders
  WHERE created_at >= '2024-01-01'
  GROUP BY user_id
)
SELECT
  au.id,
  au.name,
  au.email,
  COALESCE(uo.order_count, 0) as orders,
  COALESCE(uo.total_amount, 0) as spent
FROM active_users au
LEFT JOIN user_orders uo ON au.id = uo.user_id
ORDER BY spent DESC;`,
  },
};

export const WindowFunctionQuery: Story = {
  args: {
    title: 'Window Function Example',
    sql: `SELECT
  slot_number,
  block_hash,
  proposer_index,
  LAG(slot_number) OVER (ORDER BY slot_number) as prev_slot,
  LEAD(slot_number) OVER (ORDER BY slot_number) as next_slot,
  ROW_NUMBER() OVER (PARTITION BY proposer_index ORDER BY slot_number) as proposer_block_num
FROM beacon_blocks
WHERE slot_number >= 10000000
  AND slot_number < 10001000
ORDER BY slot_number;`,
  },
};

export const CreateTableQuery: Story = {
  args: {
    title: 'Table Creation',
    sql: `CREATE TABLE IF NOT EXISTS beacon_blocks (
  slot_number BIGINT PRIMARY KEY,
  block_hash VARCHAR(66) NOT NULL,
  parent_hash VARCHAR(66) NOT NULL,
  state_root VARCHAR(66) NOT NULL,
  proposer_index INTEGER NOT NULL,
  graffiti TEXT,
  eth1_block_hash VARCHAR(66),
  timestamp TIMESTAMP NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_proposer (proposer_index),
  INDEX idx_timestamp (timestamp)
);`,
  },
};

export const InsertQuery: Story = {
  args: {
    title: 'Data Insertion',
    sql: `INSERT INTO beacon_blocks (
  slot_number,
  block_hash,
  parent_hash,
  state_root,
  proposer_index,
  timestamp
)
SELECT
  slot_number,
  block_hash,
  parent_hash,
  state_root,
  proposer_index,
  FROM_UNIXTIME(genesis_time + slot_number * 12) as timestamp
FROM staging_blocks
WHERE slot_number BETWEEN 10000000 AND 10001000
ON DUPLICATE KEY UPDATE
  block_hash = VALUES(block_hash),
  parent_hash = VALUES(parent_hash);`,
  },
};

export const LongQuery: Story = {
  args: {
    title: 'Very Long Query',
    sql: `WITH slot_range AS (
  SELECT generate_series(10000000, 10100000) AS slot_number
),
blocks_with_gaps AS (
  SELECT
    sr.slot_number,
    bb.block_hash,
    bb.proposer_index,
    bb.timestamp,
    CASE WHEN bb.slot_number IS NULL THEN 1 ELSE 0 END as is_missed
  FROM slot_range sr
  LEFT JOIN beacon_blocks bb ON sr.slot_number = bb.slot_number
),
gap_groups AS (
  SELECT
    slot_number,
    block_hash,
    proposer_index,
    timestamp,
    is_missed,
    SUM(is_missed) OVER (ORDER BY slot_number) as gap_group
  FROM blocks_with_gaps
),
coverage_ranges AS (
  SELECT
    MIN(slot_number) as range_start,
    MAX(slot_number) as range_end,
    COUNT(*) as slot_count,
    COUNT(block_hash) as block_count,
    (COUNT(block_hash)::FLOAT / COUNT(*)::FLOAT * 100)::DECIMAL(5,2) as coverage_percent
  FROM gap_groups
  WHERE is_missed = 0
  GROUP BY gap_group
  HAVING COUNT(block_hash) > 0
)
SELECT
  range_start,
  range_end,
  slot_count,
  block_count,
  coverage_percent,
  range_end - range_start + 1 as range_size
FROM coverage_ranges
ORDER BY range_start;`,
  },
};

export const CustomTitle: Story = {
  args: {
    title: 'User Coverage Query',
    sql: 'SELECT user_id, COUNT(*) FROM sessions GROUP BY user_id;',
  },
};

export const SingleLineQuery: Story = {
  args: {
    title: 'Simple Count',
    sql: 'SELECT COUNT(*) FROM beacon_blocks;',
  },
};
