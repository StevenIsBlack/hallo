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

const mineflayer = require("mineflayer");
const fs         = require("fs");
const path       = require("path");

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
const JOIN_LOG_CHANNEL_ID   = "1442916311532441662";
const REWARD_PER_INVITE     = 5; // 1 invite = 5m
const BOT_TOKEN             = process.env.BOT_TOKEN;
const MC_USERNAME           = process.env.MC_USERNAME;
const MC_HOST               = "donutsmp.net";
const MC_PORT               = 25565;
// ──────────────────────────────────────────────────────────────────────────────

// ─── PERSISTENT STORAGE ───────────────────────────────────────────────────────
// Railway has an ephemeral filesystem but /app is writable during the container's
// lifetime. For true persistence across deploys, mount a Railway Volume at /data.
// If no volume is mounted it still works — data just resets on redeploy (not restart).
const DATA_DIR  = process.env.DATA_DIR || "/data";
const DATA_FILE = path.join(DATA_DIR, "data.json");

function ensureDataDir() {
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  } catch (e) {
    console.warn("⚠️ Could not create data directory:", e.message);
  }
}

function loadData() {
  ensureDataDir();
  try {
    if (fs.existsSync(DATA_FILE)) {
      const raw = fs.readFileSync(DATA_FILE, "utf8");
      const parsed = JSON.parse(raw);
      console.log("📂 Loaded persistent data from", DATA_FILE);
      return parsed;
    }
  } catch (e) {
    console.warn("⚠️ Could not load data file:", e.message);
  }
  return {};
}

function saveData() {
  ensureDataDir();
  try {
    const payload = {
      userInvites,
      claimedInvites,
      pendingRewards,
      joinedBefore:         [...joinedBefore],
      rewardListMessageId,
    };
    fs.writeFileSync(DATA_FILE, JSON.stringify(payload, null, 2), "utf8");
  } catch (e) {
    console.warn("⚠️ Could not save data file:", e.message);
  }
}

// Load saved data
const saved = loadData();

// total invites per user: { userId: number }
const userInvites    = saved.userInvites    || {};

// invites already paid out: { userId: number }
const claimedInvites = saved.claimedInvites || {};

// pending payout list: { userId: { ign, invites, discordTag } }
let pendingRewards   = saved.pendingRewards || {};

// everyone who has ever been in the server (rejoin detection)
const joinedBefore   = new Set(saved.joinedBefore || []);

// ID of the live payout-list message in REWARD_LOG_CHANNEL
let rewardListMessageId = saved.rewardListMessageId || null;

// ─── MINECRAFT BOT STATE ──────────────────────────────────────────────────────
// invite cache: guildId → { code: { uses, inviterId } }
const inviteCache = new Map();

let mcBot      = null;
let mcReady    = false;
let payQueue   = [];
let payRunning = false;
// ──────────────────────────────────────────────────────────────────────────────

// ─── READY ────────────────────────────────────────────────────────────────────
client.once("ready", async () => {
  console.log(`✅ ${client.user.tag} is online`);

  if (MC_USERNAME) {
    console.log("🎮 Auto-connecting Minecraft bot...");
    spawnMinecraftBot(null);
  } else {
    console.warn("⚠️ MC_USERNAME not set — Minecraft bot won't connect.");
  }

  for (const guild of client.guilds.cache.values()) {
    try {
      const invites = await guild.invites.fetch();
      const cache = {};
      invites.forEach((inv) => {
        cache[inv.code] = { uses: inv.uses, inviterId: inv.inviter?.id };
      });
      inviteCache.set(guild.id, cache);

      const members = await guild.members.fetch();
      members.forEach((m) => joinedBefore.add(m.id));
      console.log(`📋 ${guild.name}: ${invites.size} invites, ${members.size} members cached`);
    } catch (e) {
      console.error(`Cache error for guild ${guild.id}:`, e.message);
    }
  }
});

// ─── INVITE CREATED / DELETED ─────────────────────────────────────────────────
client.on("inviteCreate", (invite) => {
  const cache = inviteCache.get(invite.guild.id) || {};
  cache[invite.code] = { uses: invite.uses, inviterId: invite.inviter?.id };
  inviteCache.set(invite.guild.id, cache);
});

client.on("inviteDelete", (invite) => {
  const cache = inviteCache.get(invite.guild.id) || {};
  delete cache[invite.code];
  inviteCache.set(invite.guild.id, cache);
});

// ─── MEMBER JOIN ──────────────────────────────────────────────────────────────
client.on("guildMemberAdd", async (member) => {
  try {
    const newInvites = await member.guild.invites.fetch();
    const oldCache   = inviteCache.get(member.guild.id) || {};

    const usedInvite = newInvites.find((inv) => {
      const old = oldCache[inv.code];
      return old && inv.uses > old.uses;
    });

    const updatedCache = {};
    newInvites.forEach((inv) => {
      updatedCache[inv.code] = { uses: inv.uses, inviterId: inv.inviter?.id };
    });
    inviteCache.set(member.guild.id, updatedCache);

    const joinChannel = await member.guild.channels.fetch(JOIN_LOG_CHANNEL_ID).catch(() => null);
    const isRejoin    = joinedBefore.has(member.id);
    joinedBefore.add(member.id);

    if (usedInvite && usedInvite.inviter) {
      const inviterId = usedInvite.inviter.id;

      if (isRejoin) {
        console.log(`⚠️ Rejoin: ${member.user.tag} — NOT counted for ${usedInvite.inviter.tag}`);
        if (joinChannel) {
          await joinChannel.send({
            embeds: [
              new EmbedBuilder()
                .setColor(0xed4245)
                .setTitle("🔄 Player Rejoined — Invite Not Counted")
                .setThumbnail(member.user.displayAvatarURL({ dynamic: true }))
                .setDescription(
                  `**${member.user.username}** rejoined the server.\n` +
                  `⚠️ Invite was **not counted** for <@${inviterId}> — rejoin detected.`
                )
                .setFooter({ text: `Members: ${member.guild.memberCount}` })
                .setTimestamp(),
            ],
          });
        }
      } else {
        userInvites[inviterId] = (userInvites[inviterId] || 0) + 1;
        const total = userInvites[inviterId];
        saveData();

        console.log(`📥 ${member.user.tag} joined via ${usedInvite.inviter.tag} — ${total} invite(s) total`);

        if (joinChannel) {
          await joinChannel.send({
            embeds: [
              new EmbedBuilder()
                .setColor(0x57f287)
                .setTitle("👋 New Member Joined!")
                .setThumbnail(member.user.displayAvatarURL({ dynamic: true }))
                .setDescription(
                  `**${member.user.username}** was invited by <@${inviterId}>\n` +
                  `<@${inviterId}> now has **${total}** invite${total === 1 ? "" : "s"}.`
                )
                .setFooter({ text: `Members: ${member.guild.memberCount}` })
                .setTimestamp(),
            ],
          });
        }
      }
    } else {
      if (joinChannel) {
        await joinChannel.send({
          embeds: [
            new EmbedBuilder()
              .setColor(0xfee75c)
              .setTitle(isRejoin ? "🔄 Player Rejoined" : "👋 New Member Joined!")
              .setThumbnail(member.user.displayAvatarURL({ dynamic: true }))
              .setDescription(
                isRejoin
                  ? `**${member.user.username}** rejoined — inviter unknown.`
                  : `**${member.user.username}** joined — inviter couldn't be detected.`
              )
              .setFooter({ text: `Members: ${member.guild.memberCount}` })
              .setTimestamp(),
          ],
        });
      }
    }
  } catch (e) {
    console.error("guildMemberAdd error:", e.message);
  }
});

// ─── MEMBER LEAVE ─────────────────────────────────────────────────────────────
client.on("guildMemberRemove", async (member) => {
  joinedBefore.add(member.id);
  saveData();
  try {
    const newInvites = await member.guild.invites.fetch();
    const updatedCache = {};
    newInvites.forEach((inv) => {
      updatedCache[inv.code] = { uses: inv.uses, inviterId: inv.inviter?.id };
    });
    inviteCache.set(member.guild.id, updatedCache);
  } catch (e) {
    console.error("guildMemberRemove error:", e.message);
  }
});

// ─── COMMANDS ─────────────────────────────────────────────────────────────────
client.on("messageCreate", async (message) => {
  if (message.author.bot) return;

  const isAdmin =
    message.member?.permissions.has(PermissionFlagsBits.Administrator) ||
    message.guild?.ownerId === message.author.id;

  const args    = message.content.trim().split(/\s+/);
  const command = args[0].toLowerCase();

  // ── !reward ────────────────────────────────────────────────────────────────
  if (command === "!reward") {
    if (!isAdmin) {
      const reply = await message.reply("❌ Only admins can use this command.");
      setTimeout(() => reply.delete().catch(() => {}), 5000);
      return;
    }

    await message.delete().catch(() => {});

    const embed = new EmbedBuilder()
      .setColor(0x00c8ff)
      .setTitle("🎁 Invite Rewards")
      .setDescription(
        `Have you invited people to the server? Click **Claim Rewards** below!\n\n` +
        `**Rate:** 1 invite = ${REWARD_PER_INVITE}m\n\n` +
        `Your invite count is checked **automatically** — just enter your Minecraft IGN.`
      )
      .setFooter({ text: "Invite Rewards System" });

    const row = new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId("claim_reward")
        .setLabel("🎁 Claim Rewards")
        .setStyle(ButtonStyle.Primary)
    );

    await message.channel.send({ embeds: [embed], components: [row] });
    return;
  }

  // ── !addinvites @user amount ───────────────────────────────────────────────
  // Manually add invites to a user (admin only)
  if (command === "!addinvites") {
    if (!isAdmin) {
      const reply = await message.reply("❌ Only admins can use this command.");
      setTimeout(() => reply.delete().catch(() => {}), 5000);
      return;
    }

    const mention = message.mentions.users.first();
    const amount  = parseInt(args[2]);

    if (!mention || isNaN(amount) || amount <= 0) {
      const reply = await message.reply("❌ Usage: `!addinvites @user <amount>`\nExample: `!addinvites @David 5`");
      setTimeout(() => reply.delete().catch(() => {}), 8000);
      return;
    }

    userInvites[mention.id] = (userInvites[mention.id] || 0) + amount;
    saveData();

    await message.reply(
      `✅ Added **${amount}** invite${amount === 1 ? "" : "s"} to <@${mention.id}>.\n` +
      `They now have **${userInvites[mention.id]}** total invite${userInvites[mention.id] === 1 ? "" : "s"}.`
    );
    return;
  }

  // ── !removeinvites @user amount ───────────────────────────────────────────
  // Manually remove invites from a user (admin only)
  if (command === "!removeinvites") {
    if (!isAdmin) {
      const reply = await message.reply("❌ Only admins can use this command.");
      setTimeout(() => reply.delete().catch(() => {}), 5000);
      return;
    }

    const mention = message.mentions.users.first();
    const amount  = parseInt(args[2]);

    if (!mention || isNaN(amount) || amount <= 0) {
      const reply = await message.reply("❌ Usage: `!removeinvites @user <amount>`");
      setTimeout(() => reply.delete().catch(() => {}), 8000);
      return;
    }

    userInvites[mention.id] = Math.max(0, (userInvites[mention.id] || 0) - amount);
    saveData();

    await message.reply(
      `✅ Removed **${amount}** invite${amount === 1 ? "" : "s"} from <@${mention.id}>.\n` +
      `They now have **${userInvites[mention.id]}** total invite${userInvites[mention.id] === 1 ? "" : "s"}.`
    );
    return;
  }

  // ── !checkinvites @user ────────────────────────────────────────────────────
  if (command === "!checkinvites") {
    if (!isAdmin) {
      const reply = await message.reply("❌ Only admins can use this command.");
      setTimeout(() => reply.delete().catch(() => {}), 5000);
      return;
    }

    const mention = message.mentions.users.first() || message.author;
    const total   = userInvites[mention.id]    || 0;
    const claimed = claimedInvites[mention.id] || 0;

    await message.reply(
      `📊 **${mention.username}**\n` +
      `Total invites: **${total}** | Claimed: **${claimed}** | Available: **${total - claimed}**`
    );
    return;
  }

  // ── !mcreconnect ───────────────────────────────────────────────────────────
  if (command === "!mcreconnect") {
    if (!isAdmin) return;
    await message.delete().catch(() => {});
    await spawnMinecraftBot(message.channel);
    return;
  }

  // ── !mcstatus ──────────────────────────────────────────────────────────────
  if (command === "!mcstatus") {
    if (!isAdmin) return;
    if (!mcBot || !mcReady) {
      await message.reply("🔴 Minecraft bot is **not connected**.");
    } else {
      await message.reply(`🟢 Minecraft bot is **online** as \`${mcBot.username}\` on \`${MC_HOST}:${MC_PORT}\``);
    }
    return;
  }

  // ── !mckick ────────────────────────────────────────────────────────────────
  if (command === "!mckick") {
    if (!isAdmin) return;
    if (mcBot) {
      mcBot.quit();
      mcBot   = null;
      mcReady = false;
      await message.reply("✅ Minecraft bot disconnected.");
    } else {
      await message.reply("❌ No Minecraft bot connected.");
    }
    return;
  }
});

// ─── Spawn the Mineflayer bot ─────────────────────────────────────────────────
async function spawnMinecraftBot(feedbackChannel) {
  if (mcBot) {
    mcBot.quit();
    mcBot   = null;
    mcReady = false;
  }

  if (feedbackChannel) {
    await feedbackChannel.send("🔄 Connecting Minecraft bot...");
  }

  // Microsoft auth token cache — persisted to DATA_DIR so login is only needed once
  const authCacheDir = path.join(DATA_DIR, "auth_cache");
  try {
    if (!fs.existsSync(authCacheDir)) fs.mkdirSync(authCacheDir, { recursive: true });
  } catch (e) {
    console.warn("Could not create auth cache dir:", e.message);
  }

  try {
    mcBot = mineflayer.createBot({
      host:           MC_HOST,
      port:           MC_PORT,
      username:       MC_USERNAME,
      auth:           "microsoft",
      version:        "1.20.5",
      // Cache the Microsoft token so we never need to verify again after the first login
      profilesFolder: authCacheDir,
    });

    // Only fires the very first time — never again once token is cached
    mcBot.on("microsoftDeviceCode", async (data) => {
      const msg =
        `🔐 **One-time Microsoft Verification**\n\n` +
        `1. Go to: **${data.verificationUri}**\n` +
        `2. Enter code: \`${data.userCode}\`\n` +
        `3. Sign in with the Microsoft account linked to your Minecraft Java account\n\n` +
        `✅ After this you'll **never need to verify again** — the token is saved permanently!`;

      console.log(`🔐 Microsoft verification — code: ${data.userCode} at ${data.verificationUri}`);

      if (feedbackChannel) {
        await feedbackChannel.send(msg);
      } else {
        const logChannel = await client.channels.fetch(REWARD_LOG_CHANNEL_ID).catch(() => null);
        if (logChannel) await logChannel.send(msg);
      }
    });

    mcBot.once("spawn", async () => {
      mcReady = true;
      console.log(`🎮 Minecraft bot spawned as ${mcBot.username} on ${MC_HOST}:${MC_PORT}`);

      const successMsg = `✅ Minecraft bot **${mcBot.username}** is online on **${MC_HOST}** and ready to pay rewards!`;
      if (feedbackChannel) {
        await feedbackChannel.send(successMsg);
      } else {
        const logChannel = await client.channels.fetch(REWARD_LOG_CHANNEL_ID).catch(() => null);
        if (logChannel) await logChannel.send(successMsg);
      }

      // Simulate slight human movement so anti-bot doesn't kick us
      setTimeout(() => { if (mcBot && mcReady) mcBot.look(mcBot.entity.yaw + 0.3, mcBot.entity.pitch, false); }, 2000);
      setTimeout(() => { if (mcBot && mcReady) mcBot.look(mcBot.entity.yaw - 0.1, 0, false); }, 5000);

      // Fire any queued payments
      if (payQueue.length > 0) {
        console.log(`⏳ Processing ${payQueue.length} queued payment(s)...`);
        runPayQueue();
      }
    });

    mcBot.on("kicked", async (reason) => {
      mcReady = false;
      let reasonText = reason;
      try {
        const parsed = typeof reason === "string" ? JSON.parse(reason) : reason;
        reasonText = parsed?.text || parsed?.translate || JSON.stringify(parsed);
      } catch { reasonText = String(reason); }

      console.log(`⚠️ Minecraft bot was kicked: ${reasonText}`);
      const logChannel = await client.channels.fetch(REWARD_LOG_CHANNEL_ID).catch(() => null);
      if (logChannel) await logChannel.send(`⚠️ Minecraft bot was **kicked**: \`${reasonText}\` — reconnecting in 60s...`);

      mcBot = null;
      setTimeout(() => spawnMinecraftBot(null), 60000);
    });

    mcBot.on("error", (err) => {
      mcReady = false;
      console.error("Minecraft bot error:", err.message);
    });

    mcBot.on("end", () => {
      if (mcReady) {
        mcReady = false;
        console.log("Minecraft bot disconnected — reconnecting in 30s...");
        setTimeout(() => spawnMinecraftBot(null), 30000);
      }
    });

  } catch (e) {
    console.error("Failed to start Minecraft bot:", e.message);
    if (feedbackChannel) await feedbackChannel.send(`❌ Failed to start Minecraft bot: \`${e.message}\``);
  }
}

// ─── Run pay queue ─────────────────────────────────────────────────────────────
async function runPayQueue() {
  if (payRunning || payQueue.length === 0) return;
  payRunning = true;

  // Merge duplicate IGNs into a single payment so shared accounts get paid correctly
  // e.g. two people claim for "Steve" with 10m and 15m → one /pay Steve 25m
  const merged = {};
  for (const { ign, amount } of payQueue) {
    const key = ign.toLowerCase();
    merged[key] = { ign, amount: (merged[key]?.amount || 0) + amount };
  }
  payQueue = []; // clear original queue, we're using merged now

  for (const { ign, amount } of Object.values(merged)) {
    if (!mcBot || !mcReady) {
      // Bot went offline mid-queue — re-add remaining to queue for later
      payQueue.push({ ign, amount });
      console.log(`⚠️ MC bot not ready — re-queued payment for ${ign}`);
      continue;
    }

    const cmd = `/pay ${ign} ${amount}m`;

    try {
      mcBot.chat(cmd);
      console.log(`💰 Sent: ${cmd}`);
    } catch (e) {
      console.error(`Failed to send pay command for ${ign}:`, e.message);
      payQueue.push({ ign, amount }); // re-queue on failure
    }

    await new Promise((r) => setTimeout(r, 1500));
  }

  payRunning = false;
}

// ─── INTERACTIONS ─────────────────────────────────────────────────────────────
client.on("interactionCreate", async (interaction) => {

  // ── Claim button → modal ───────────────────────────────────────────────────
  if (interaction.isButton() && interaction.customId === "claim_reward") {
    const modal = new ModalBuilder()
      .setCustomId("reward_modal")
      .setTitle("Claim Your Invite Rewards");

    modal.addComponents(
      new ActionRowBuilder().addComponents(
        new TextInputBuilder()
          .setCustomId("ign")
          .setLabel("Your Minecraft Username")
          .setStyle(TextInputStyle.Short)
          .setPlaceholder("e.g. Steve")
          .setRequired(true)
      )
    );

    await interaction.showModal(modal);
    return;
  }

  // ── Modal submit ───────────────────────────────────────────────────────────
  if (interaction.isModalSubmit() && interaction.customId === "reward_modal") {
    await interaction.deferReply({ ephemeral: true });

    const ign        = interaction.fields.getTextInputValue("ign").trim();
    const userId     = interaction.user.id;
    const discordTag = interaction.user.tag;

    const total     = userInvites[userId]    || 0;
    const claimed   = claimedInvites[userId] || 0;
    const claimable = total - claimed;

    if (pendingRewards[userId]) {
      const p = pendingRewards[userId];
      await interaction.editReply({
        content:
          `⏳ You already have a pending claim!\n\n` +
          `**IGN:** ${p.ign} | **Invites:** ${p.invites} | **Reward:** ${p.invites * REWARD_PER_INVITE}m\n\n` +
          `Wait for it to be paid before claiming again.`,
      });
      return;
    }

    if (claimable <= 0) {
      await interaction.editReply({
        content:
          `❌ You have no unclaimed invites.\n\n` +
          `📊 Total invites: **${total}** | Already claimed: **${claimed}**\n\n` +
          `Invite more players to earn rewards!`,
      });
      return;
    }

    pendingRewards[userId] = { ign, invites: claimable, discordTag };
    saveData();
    await updateRewardList(interaction.client, interaction.guild);

    await interaction.editReply({
      content:
        `✅ **Claim submitted!**\n\n` +
        `🎮 **Minecraft IGN:** ${ign}\n` +
        `🎟️ **Invites:** ${claimable}\n` +
        `💰 **Reward:** ${claimable * REWARD_PER_INVITE}m\n\n` +
        `You'll get a DM when your reward has been sent in-game! 🎉`,
    });
    return;
  }

  // ── Mark individual paid ───────────────────────────────────────────────────
  if (interaction.isButton() && interaction.customId.startsWith("mark_paid_")) {
    if (
      !interaction.member.permissions.has(PermissionFlagsBits.Administrator) &&
      interaction.guild.ownerId !== interaction.user.id
    ) {
      await interaction.reply({ content: "❌ Admins only.", ephemeral: true });
      return;
    }

    const targetId = interaction.customId.replace("mark_paid_", "");
    if (!pendingRewards[targetId]) {
      await interaction.reply({ content: "❌ Not found in pending list.", ephemeral: true });
      return;
    }

    const data   = pendingRewards[targetId];
    const amount = data.invites * REWARD_PER_INVITE;

    payQueue.push({ ign: data.ign, amount });
    if (mcBot && mcReady) runPayQueue();

    await payUser(interaction.client, interaction.guild, targetId);
    await updateRewardList(interaction.client, interaction.guild);

    const status = (!mcBot || !mcReady)
      ? `⚠️ MC bot offline — payment queued for when it reconnects.`
      : `✅ Paying **${data.ign}** ${amount}m in-game now!`;

    await interaction.reply({ content: status, ephemeral: true });
    return;
  }

  // ── Pay all ────────────────────────────────────────────────────────────────
  if (interaction.isButton() && interaction.customId === "pay_all") {
    if (
      !interaction.member.permissions.has(PermissionFlagsBits.Administrator) &&
      interaction.guild.ownerId !== interaction.user.id
    ) {
      await interaction.reply({ content: "❌ Admins only.", ephemeral: true });
      return;
    }

    const all = Object.keys(pendingRewards);
    if (all.length === 0) {
      await interaction.reply({ content: "✅ Nothing pending!", ephemeral: true });
      return;
    }

    for (const uid of all) {
      const data   = pendingRewards[uid];
      const amount = data.invites * REWARD_PER_INVITE;
      payQueue.push({ ign: data.ign, amount });
    }

    for (const uid of all) {
      await payUser(interaction.client, interaction.guild, uid);
    }

    await updateRewardList(interaction.client, interaction.guild);
    if (mcBot && mcReady) runPayQueue();

    const status = (!mcBot || !mcReady)
      ? `✅ ${all.length} payment(s) queued — will be sent when MC bot reconnects.`
      : `✅ Paying ${all.length} player(s) in-game now!`;

    await interaction.reply({ content: status, ephemeral: true });
    return;
  }
});

// ─── Pay one user (DM + clear pending) ────────────────────────────────────────
async function payUser(client, guild, userId) {
  const data = pendingRewards[userId];
  if (!data) return;

  // Reset both counters to 0 so future invites can be claimed fresh
  userInvites[userId]    = 0;
  claimedInvites[userId] = 0;
  delete pendingRewards[userId];
  saveData();

  try {
    const member = await guild.members.fetch(userId);
    await member.send(
      `✅ **Your invite rewards are being paid in-game!**\n\n` +
      `🎮 **IGN:** ${data.ign}\n` +
      `🎟️ **Invites:** ${data.invites}\n` +
      `💰 **Amount:** ${data.invites * REWARD_PER_INVITE}m\n\n` +
      `Please leave a vouch in **#Vouches-rewards**! 🎉`
    );
  } catch {
    console.log(`⚠️ Could not DM ${userId}`);
  }
}

// ─── Build / update the payout list ──────────────────────────────────────────
async function updateRewardList(client, guild) {
  const logChannel = await client.channels.fetch(REWARD_LOG_CHANNEL_ID).catch(() => null);
  if (!logChannel) return console.error("❌ Reward log channel not found.");

  const unpaid = Object.entries(pendingRewards);

  const mcStatus = (!mcBot || !mcReady)
    ? "🔴 MC Bot Offline — auto-reconnecting..."
    : `🟢 MC Bot Online as \`${mcBot.username}\``;

  const embed = new EmbedBuilder()
    .setColor(0xf5a623)
    .setTitle("🎁 Invite Reward Queue")
    .setTimestamp()
    .setFooter({ text: mcStatus });

  if (unpaid.length === 0) {
    embed.setDescription("✅ Queue is empty — all paid!");
  } else {
    const lines = unpaid.map(([, data], i) => {
      const reward = data.invites * REWARD_PER_INVITE;
      return `**${i + 1}.** ${data.discordTag} · IGN: \`${data.ign}\` · **${data.invites}** invite${data.invites === 1 ? "" : "s"} → **${reward}m**`;
    });
    const totalM = unpaid.reduce((s, [, d]) => s + d.invites * REWARD_PER_INVITE, 0);
    embed.setDescription(lines.join("\n"));
    embed.addFields({ name: "Total to pay", value: `**${totalM}m**`, inline: false });
  }

  const components = [];

  if (unpaid.length > 0) {
    const chunks = chunkArray(unpaid.slice(0, 20), 4);
    for (const chunk of chunks) {
      components.push(
        new ActionRowBuilder().addComponents(
          chunk.map(([uid, data]) =>
            new ButtonBuilder()
              .setCustomId(`mark_paid_${uid}`)
              .setLabel(`✅ ${data.ign}`)
              .setStyle(ButtonStyle.Success)
          )
        )
      );
    }

    components.push(
      new ActionRowBuilder().addComponents(
        new ButtonBuilder()
          .setCustomId("pay_all")
          .setLabel("💰 Pay All & Clear Queue")
          .setStyle(ButtonStyle.Danger)
      )
    );
  }

  if (rewardListMessageId) {
    try {
      const existing = await logChannel.messages.fetch(rewardListMessageId);
      await existing.edit({ embeds: [embed], components });
      return;
    } catch {
      // message was deleted, send new one
    }
  }

  const sent = await logChannel.send({ embeds: [embed], components });
  rewardListMessageId = sent.id;
  saveData();
}

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

client.login(BOT_TOKEN);
