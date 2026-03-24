const {
  Client,
  GatewayIntentBits,
  Partials,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  ModalBuilder,
  TextInputBuilder,
  TextInputStyle,
  EmbedBuilder,
  PermissionFlagsBits,
} = require("discord.js");

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.GuildInvites,
    GatewayIntentBits.DirectMessages,
  ],
  partials: [Partials.Channel, Partials.Message],
});

// ─── CONFIG ───────────────────────────────────────────────────────────────────
const REWARD_LOG_CHANNEL_ID = "1466514242558759278";
const REWARD_PER_INVITE = 5; // 1 invite = 5m
const BOT_TOKEN = process.env.BOT_TOKEN;
// ──────────────────────────────────────────────────────────────────────────────

// invite tracking: guildId -> { inviteCode: { uses, inviterId } }
const inviteCache = new Map();

// total invites earned per user: { userId: number }
const userInvites = {};

// invites already claimed for rewards: { userId: number }
const claimedInvites = {};

// pending reward list: { userId: { ign, invites, discordTag } }
let pendingRewards = {};

// message ID of the reward list in the log channel
let rewardListMessageId = null;

// ─── READY: cache all current invites ────────────────────────────────────────
client.once("ready", async () => {
  console.log(`✅ Logged in as ${client.user.tag}`);

  for (const guild of client.guilds.cache.values()) {
    try {
      const invites = await guild.invites.fetch();
      const cache = {};
      invites.forEach((inv) => {
        cache[inv.code] = { uses: inv.uses, inviterId: inv.inviter?.id };
      });
      inviteCache.set(guild.id, cache);
      console.log(`📋 Cached ${invites.size} invites for ${guild.name}`);
    } catch (e) {
      console.error(`Could not cache invites for guild ${guild.id}:`, e.message);
    }
  }
});

// ─── New invite created ───────────────────────────────────────────────────────
client.on("inviteCreate", (invite) => {
  const cache = inviteCache.get(invite.guild.id) || {};
  cache[invite.code] = { uses: invite.uses, inviterId: invite.inviter?.id };
  inviteCache.set(invite.guild.id, cache);
});

// ─── Invite deleted ───────────────────────────────────────────────────────────
client.on("inviteDelete", (invite) => {
  const cache = inviteCache.get(invite.guild.id) || {};
  delete cache[invite.code];
  inviteCache.set(invite.guild.id, cache);
});

// ─── Member joins → detect who invited them ──────────────────────────────────
client.on("guildMemberAdd", async (member) => {
  try {
    const newInvites = await member.guild.invites.fetch();
    const oldCache = inviteCache.get(member.guild.id) || {};

    // Find the invite whose use count increased
    const usedInvite = newInvites.find((inv) => {
      const old = oldCache[inv.code];
      return old && inv.uses > old.uses;
    });

    // Update the cache to reflect new use counts
    const updatedCache = {};
    newInvites.forEach((inv) => {
      updatedCache[inv.code] = { uses: inv.uses, inviterId: inv.inviter?.id };
    });
    inviteCache.set(member.guild.id, updatedCache);

    if (usedInvite && usedInvite.inviter) {
      const inviterId = usedInvite.inviter.id;
      userInvites[inviterId] = (userInvites[inviterId] || 0) + 1;
      console.log(
        `📥 ${member.user.tag} joined via ${usedInvite.inviter.tag}'s invite. ` +
        `They now have ${userInvites[inviterId]} total invite(s).`
      );
    }
  } catch (e) {
    console.error("Error tracking invite on member join:", e.message);
  }
});

// ─── Member leaves → update cache ────────────────────────────────────────────
client.on("guildMemberRemove", async (member) => {
  try {
    const newInvites = await member.guild.invites.fetch();
    const updatedCache = {};
    newInvites.forEach((inv) => {
      updatedCache[inv.code] = { uses: inv.uses, inviterId: inv.inviter?.id };
    });
    inviteCache.set(member.guild.id, updatedCache);
  } catch (e) {
    console.error("Error updating cache on member remove:", e.message);
  }
});

// ─── !reward COMMAND ──────────────────────────────────────────────────────────
client.on("messageCreate", async (message) => {
  if (message.author.bot) return;
  if (message.content.toLowerCase() !== "!reward") return;

  if (
    !message.member.permissions.has(PermissionFlagsBits.Administrator) &&
    message.guild.ownerId !== message.author.id
  ) {
    const reply = await message.reply("❌ You don't have permission to use this command.");
    setTimeout(() => reply.delete().catch(() => {}), 5000);
    return;
  }

  await message.delete().catch(() => {});

  const embed = new EmbedBuilder()
    .setTitle("🎁 Invite Rewards")
    .setDescription(
      `Have you invited people to the server? Click **Claim Rewards** below!\n\n` +
      `**Rate:** 1 invite = ${REWARD_PER_INVITE}m\n\n` +
      `Your invite count is checked **automatically** — just enter your IGN.`
    )
    .setColor(0x00c8ff)
    .setFooter({ text: "Invite Rewards System" });

  const row = new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId("claim_reward")
      .setLabel("🎁 Claim Rewards")
      .setStyle(ButtonStyle.Primary)
  );

  await message.channel.send({ embeds: [embed], components: [row] });
});

// ─── INTERACTIONS ─────────────────────────────────────────────────────────────
client.on("interactionCreate", async (interaction) => {

  // ── Claim button → show modal ─────────────────────────────────────────────
  if (interaction.isButton() && interaction.customId === "claim_reward") {
    const modal = new ModalBuilder()
      .setCustomId("reward_modal")
      .setTitle("Claim Your Invite Rewards");

    const ignInput = new TextInputBuilder()
      .setCustomId("ign")
      .setLabel("Your In-Game Username")
      .setStyle(TextInputStyle.Short)
      .setPlaceholder("Enter your IGN...")
      .setRequired(true);

    modal.addComponents(new ActionRowBuilder().addComponents(ignInput));
    await interaction.showModal(modal);
    return;
  }

  // ── Modal submit ──────────────────────────────────────────────────────────
  if (interaction.isModalSubmit() && interaction.customId === "reward_modal") {
    await interaction.deferReply({ ephemeral: true });

    const ign = interaction.fields.getTextInputValue("ign").trim();
    const userId = interaction.user.id;
    const discordTag = interaction.user.tag;

    const totalInvites = userInvites[userId] || 0;
    const alreadyClaimed = claimedInvites[userId] || 0;
    const claimableInvites = totalInvites - alreadyClaimed;

    // Already has a pending claim
    if (pendingRewards[userId]) {
      await interaction.editReply({
        content:
          `⏳ You already have a pending reward claim for **${pendingRewards[userId].invites} invite(s)**.\n` +
          `Please wait for it to be paid before claiming again.`,
      });
      return;
    }

    // Not enough invites
    if (claimableInvites <= 0) {
      await interaction.editReply({
        content:
          `❌ You don't have any unclaimed invites.\n\n` +
          `**Your total invites:** ${totalInvites}\n` +
          `**Already claimed:** ${alreadyClaimed}\n\n` +
          `Invite more people to the server to earn rewards!`,
      });
      return;
    }

    // Save the pending claim
    pendingRewards[userId] = { ign, invites: claimableInvites, discordTag };

    await updateRewardList(interaction.client, interaction.guild);

    await interaction.editReply({
      content:
        `✅ Reward claim submitted!\n\n` +
        `**IGN:** ${ign}\n` +
        `**Invites:** ${claimableInvites}\n` +
        `**Reward:** ${claimableInvites * REWARD_PER_INVITE}m\n\n` +
        `You'll get a DM once you've been paid! 🎉`,
    });
    return;
  }

  // ── Mark individual as paid ───────────────────────────────────────────────
  if (interaction.isButton() && interaction.customId.startsWith("mark_paid_")) {
    if (
      !interaction.member.permissions.has(PermissionFlagsBits.Administrator) &&
      interaction.guild.ownerId !== interaction.user.id
    ) {
      await interaction.reply({ content: "❌ Admins only.", ephemeral: true });
      return;
    }

    const targetUserId = interaction.customId.replace("mark_paid_", "");

    if (!pendingRewards[targetUserId]) {
      await interaction.reply({ content: "❌ User not found in pending list.", ephemeral: true });
      return;
    }

    await payUser(interaction.client, interaction.guild, targetUserId);
    await updateRewardList(interaction.client, interaction.guild);
    await interaction.reply({ content: `✅ Marked as paid and DM sent!`, ephemeral: true });
    return;
  }

  // ── Pay all ───────────────────────────────────────────────────────────────
  if (interaction.isButton() && interaction.customId === "pay_all") {
    if (
      !interaction.member.permissions.has(PermissionFlagsBits.Administrator) &&
      interaction.guild.ownerId !== interaction.user.id
    ) {
      await interaction.reply({ content: "❌ Admins only.", ephemeral: true });
      return;
    }

    const unpaidUsers = Object.keys(pendingRewards);

    if (unpaidUsers.length === 0) {
      await interaction.reply({ content: "✅ No pending rewards to clear.", ephemeral: true });
      return;
    }

    for (const userId of unpaidUsers) {
      await payUser(interaction.client, interaction.guild, userId);
    }

    await updateRewardList(interaction.client, interaction.guild);
    await interaction.reply({
      content: `✅ All ${unpaidUsers.length} reward(s) paid and users DM'd!`,
      ephemeral: true,
    });
    return;
  }
});

// ─── Pay a single user ────────────────────────────────────────────────────────
async function payUser(client, guild, userId) {
  const data = pendingRewards[userId];
  if (!data) return;

  // Mark their invites as claimed so they don't double-dip
  claimedInvites[userId] = (claimedInvites[userId] || 0) + data.invites;

  // DM the user
  try {
    const member = await guild.members.fetch(userId);
    await member.send(
      `✅ **Your invite rewards have been paid!**\n\n` +
      `**IGN:** ${data.ign}\n` +
      `**Invites:** ${data.invites}\n` +
      `**Amount:** ${data.invites * REWARD_PER_INVITE}m\n\n` +
      `Please leave a vouch in **#Vouches-rewards**! 🎉`
    );
  } catch (e) {
    console.log(`⚠️ Could not DM user ${userId} — they may have DMs disabled.`);
  }

  delete pendingRewards[userId];
}

// ─── Build & update the reward list message ───────────────────────────────────
async function updateRewardList(client, guild) {
  const logChannel = await client.channels.fetch(REWARD_LOG_CHANNEL_ID).catch(() => null);
  if (!logChannel) return console.error("❌ Could not find reward log channel.");

  const unpaid = Object.entries(pendingRewards);

  const embed = new EmbedBuilder()
    .setTitle("📋 Pending Invite Rewards")
    .setColor(0xffa500)
    .setTimestamp()
    .setFooter({ text: "Click ✅ next to a player to mark them as paid" });

  if (unpaid.length === 0) {
    embed.setDescription("✅ No pending rewards — all clear!");
  } else {
    const lines = unpaid.map(
      ([, data], i) =>
        `**${i + 1}.** ${data.discordTag} | IGN: \`${data.ign}\` | ` +
        `Invites: **${data.invites}** | Reward: **${data.invites * REWARD_PER_INVITE}m**`
    );
    embed.setDescription(lines.join("\n"));
  }

  const components = [];

  if (unpaid.length > 0) {
    // Individual pay buttons in rows of 4 (max 20 people shown)
    const chunks = chunkArray(unpaid.slice(0, 20), 4);
    for (const chunk of chunks) {
      const row = new ActionRowBuilder().addComponents(
        chunk.map(([userId, data]) =>
          new ButtonBuilder()
            .setCustomId(`mark_paid_${userId}`)
            .setLabel(`✅ ${data.ign}`)
            .setStyle(ButtonStyle.Success)
        )
      );
      components.push(row);
    }

    // Pay All button
    components.push(
      new ActionRowBuilder().addComponents(
        new ButtonBuilder()
          .setCustomId("pay_all")
          .setLabel("💰 Pay All & Clear List")
          .setStyle(ButtonStyle.Danger)
      )
    );
  }

  // Edit existing message or post a new one
  if (rewardListMessageId) {
    try {
      const existing = await logChannel.messages.fetch(rewardListMessageId);
      await existing.edit({ embeds: [embed], components });
      return;
    } catch (e) {
      // Message was deleted — fall through to send a new one
    }
  }

  const sent = await logChannel.send({ embeds: [embed], components });
  rewardListMessageId = sent.id;
}

function chunkArray(arr, size) {
  const result = [];
  for (let i = 0; i < arr.length; i += size) result.push(arr.slice(i, i + size));
  return result;
}

client.login(BOT_TOKEN);
