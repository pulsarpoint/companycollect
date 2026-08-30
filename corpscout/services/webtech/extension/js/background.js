'use strict'

/* globals chrome, importScripts, Wappalyzer */

importScripts(chrome.runtime.getURL('js/wappalyzer.js'))

const EXTENSION_VERSION = '1.4.1'
const ANALYSIS_DEADLINE_MS = 30000
const REPORT_FINALIZE_DELAY_MS = 250
const REPORT_RETRY_MS = 5000
const MAX_EXTERNAL_SCRIPTS_PER_PAGE = 25
const MAX_EXTERNAL_SCRIPT_CHARACTERS = 100000
const MAX_XHR_HOSTS_PER_PAGE = 100

const pageStates = new Map()

const technologiesReady = loadTechnologyDefinitions()
const runtimeConfigReady = loadRuntimeConfig()

function normalizePageUrl(url) {
  return String(url || '').split('#')[0]
}

function isWebUrl(url) {
  return /^https?:\/\//i.test(url || '')
}

function createPageState(url, pageToken = '') {
  return {
    url: normalizePageUrl(url),
    pageToken,
    registeredAt: Date.now(),
    detections: [],
    detectionKeys: new Set(),
    analyzedScriptUrls: new Set(),
    analyzedRequestHosts: new Set(),
    triggeredRequirements: new Set(),
    technologies: [],
    analysisStatus: 'running',
    currentStage: 'content_started',
    stageTimingsMs: {},
    failureStage: '',
    errorMessage: '',
    finalized: false,
    finalizedAt: 0,
    finalizedElapsedMs: 0,
    lastReportedSignature: '',
    analysisDeadlineTimer: undefined,
    reportTimer: undefined,
  }
}

function getPageState(tabId, url, pageToken = '') {
  const normalizedUrl = normalizePageUrl(url)
  const currentState = pageStates.get(tabId)

  if (currentState && pageToken && currentState.pageToken === pageToken) {
    currentState.url = normalizedUrl

    return currentState
  }

  if (!currentState || currentState.url !== normalizedUrl) {
    if (currentState?.analysisDeadlineTimer) {
      clearTimeout(currentState.analysisDeadlineTimer)
    }

    if (currentState?.reportTimer) {
      clearTimeout(currentState.reportTimer)
    }

    const nextState = createPageState(normalizedUrl, pageToken)
    pageStates.set(tabId, nextState)

    return nextState
  }

  if (pageToken) {
    currentState.pageToken = pageToken
  }

  return currentState
}

function deletePageState(tabId) {
  const state = pageStates.get(tabId)

  if (state?.analysisDeadlineTimer) {
    clearTimeout(state.analysisDeadlineTimer)
  }

  if (state?.reportTimer) {
    clearTimeout(state.reportTimer)
  }

  pageStates.delete(tabId)
}

function createDetectionKey({ technology, pattern = {}, version = '' }) {
  return [
    technology?.name || '',
    pattern.type || '',
    pattern.value || '',
    pattern.match || '',
    version,
  ].join('|')
}

function addUniqueDetections(state, detections) {
  let changed = false

  for (const detection of detections || []) {
    if (!detection?.technology?.name) {
      continue
    }

    const key = createDetectionKey(detection)

    if (state.detectionKeys.has(key)) {
      continue
    }

    state.detectionKeys.add(key)
    state.detections.push(detection)
    changed = true
  }

  return changed
}

function getResolvedTechnologies(detections) {
  return Wappalyzer.resolve(detections)
    .map(({ name, slug, categories, confidence, version }) => ({
      name,
      slug,
      categories: categories.map(({ id, name: categoryName, slug }) => ({
        id,
        name: categoryName,
        slug,
      })),
      confidence,
      version,
    }))
    .sort(({ name: a }, { name: b }) => a.localeCompare(b))
}

function createReportSignature(payload) {
  return JSON.stringify(payload)
}

async function postTechnologyReport(tabId, state) {
  if (pageStates.get(tabId) !== state) {
    return
  }

  if (!state.pageToken) {
    state.reportTimer = setTimeout(
      () => postTechnologyReport(tabId, state),
      REPORT_RETRY_MS
    )

    return
  }

  const { callbackUrl } = await runtimeConfigReady
  const observedElapsedMs = Math.max(
    0,
    ...Object.values(state.stageTimingsMs).filter(Number.isFinite)
  )
  state.stageTimingsMs.report_post_started ??= Math.max(
    observedElapsedMs,
    state.finalizedElapsedMs + Math.max(0, Date.now() - state.finalizedAt)
  )
  const payload = {
    schema_version: 3,
    analysis_complete: state.analysisStatus === 'complete',
    analysis_status: state.analysisStatus,
    extension_version: EXTENSION_VERSION,
    page_token: state.pageToken,
    url: state.url,
    technologies: state.technologies,
    failure_stage: state.failureStage || null,
    error_message: state.errorMessage,
    stage_timings_ms: state.stageTimingsMs,
  }
  const signature = createReportSignature(payload)

  if (signature === state.lastReportedSignature) {
    return
  }

  try {
    const response = await fetch(callbackUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
    })

    if (!response.ok) {
      throw new Error(
        `Technology API responded with HTTP ${response.status}`
      )
    }

    state.lastReportedSignature = signature
  } catch (error) {
    console.error(`Could not POST technologies to ${callbackUrl}`, error)

    state.reportTimer = setTimeout(
      () => postTechnologyReport(tabId, state),
      REPORT_RETRY_MS
    )
  }
}

function finalizeAnalysis(
  tabId,
  state,
  {
    analysisStatus,
    failureStage = '',
    errorMessage = '',
    stageTimingsMs = {},
  }
) {
  if (state.finalized) {
    return
  }

  state.finalized = true
  state.analysisStatus = analysisStatus
  state.failureStage = failureStage
  state.errorMessage = errorMessage
  state.stageTimingsMs = { ...state.stageTimingsMs, ...stageTimingsMs }
  state.finalizedAt = Date.now()
  state.finalizedElapsedMs = Math.max(
    0,
    ...Object.values(state.stageTimingsMs).filter(Number.isFinite)
  )

  if (state.analysisDeadlineTimer) {
    clearTimeout(state.analysisDeadlineTimer)
    state.analysisDeadlineTimer = undefined
  }

  state.reportTimer = setTimeout(
    () => postTechnologyReport(tabId, state),
    REPORT_FINALIZE_DELAY_MS
  )
}

function ensureAnalysisDeadline(tabId, state) {
  if (
    !state.pageToken ||
    state.finalized ||
    state.analysisDeadlineTimer
  ) {
    return
  }

  state.analysisDeadlineTimer = setTimeout(
    () => {
      state.stageTimingsMs.analysis_timed_out = Math.max(
        0,
        Date.now() - state.registeredAt
      )
      finalizeAnalysis(tabId, state, {
        analysisStatus: 'partial',
        failureStage: state.currentStage,
        errorMessage: `Extension analysis exceeded ${ANALYSIS_DEADLINE_MS}ms`,
      })
    },
    ANALYSIS_DEADLINE_MS
  )
}

function getRequiredTechnologies(
  requiredTechnologyName,
  requiredCategoryId,
  explicitTechnologyNames = []
) {
  if (explicitTechnologyNames.length) {
    return explicitTechnologyNames
      .map((name) => Wappalyzer.getTechnology(name))
      .filter(Boolean)
  }

  if (requiredTechnologyName) {
    return (
      Wappalyzer.requires.find(
        ({ name }) => name === requiredTechnologyName
      )?.technologies || []
    )
  }

  if (requiredCategoryId !== undefined && requiredCategoryId !== null) {
    const categoryId = Number.parseInt(requiredCategoryId, 10)

    return (
      Wappalyzer.categoryRequires.find(
        (requirement) => requirement.categoryId === categoryId
      )?.technologies || []
    )
  }

  return Wappalyzer.technologies
}

function getRequirementsActivatedBy(technologyNames) {
  const detectedNames = new Set(technologyNames)
  const detectedCategoryIds = new Set(
    technologyNames
      .map((name) => Wappalyzer.getTechnology(name))
      .filter(Boolean)
      .flatMap(({ categories }) => categories)
  )

  return [
    ...Wappalyzer.requires.filter(({ name }) => detectedNames.has(name)),
    ...Wappalyzer.categoryRequires.filter(({ categoryId }) =>
      detectedCategoryIds.has(categoryId)
    ),
  ]
}

function requestRequiredTechnologySignals(tabId, state, technologyNames) {
  const newRequirements = getRequirementsActivatedBy(technologyNames)
    .filter(({ name, categoryId }) => {
      const key = name ? `technology:${name}` : `category:${categoryId}`

      if (state.triggeredRequirements.has(key)) {
        return false
      }

      state.triggeredRequirements.add(key)

      return true
    })

  if (!newRequirements.length) {
    return
  }

  chrome.tabs.sendMessage(
    tabId,
    {
      source: 'background.js',
      func: 'analyzeRequires',
      args: [state.url, newRequirements],
    },
    { frameId: 0 },
    () => void chrome.runtime.lastError
  )
}

function recordDetections(
  tabId,
  frameId,
  url,
  detections = [],
  pageToken = ''
) {
  if (typeof tabId !== 'number' || tabId < 0 || frameId > 0 || !isWebUrl(url)) {
    return
  }

  const state = getPageState(tabId, url, pageToken)

  if (state.finalized || !addUniqueDetections(state, detections)) {
    return
  }

  state.technologies = getResolvedTechnologies(state.detections)

  requestRequiredTechnologySignals(
    tabId,
    state,
    state.technologies.map(({ name }) => name)
  )

}

function normalizeCookieNames(cookies = {}) {
  return Object.fromEntries(
    Object.entries(cookies).map(([name, values]) => [
      name.toLowerCase(),
      values,
    ])
  )
}

async function addAccessibleCookies(url, items) {
  const cookies = normalizeCookieNames(items.cookies)
  const browserCookies = await chrome.cookies.getAll({ url })

  for (const { name, value } of browserCookies) {
    cookies[name.toLowerCase()] = [value]
  }

  for (const name of Object.keys(cookies)) {
    if (/^_ga_[a-z0-9]+$/i.test(name)) {
      cookies['_ga_*'] = cookies[name]
      delete cookies[name]
    }
  }

  items.cookies = cookies
}

function analyzeJavaScriptSignals(
  signals,
  requiredTechnologyName,
  requiredCategoryId,
  explicitTechnologyNames
) {
  const technologies = getRequiredTechnologies(
    requiredTechnologyName,
    requiredCategoryId,
    explicitTechnologyNames
  )
  const technologiesByName = new Map(
    technologies.map((technology) => [technology.name, technology])
  )

  return signals.flatMap(({ name, chain, value }) => {
    const technology = technologiesByName.get(name)

    return technology
      ? Wappalyzer.analyzeManyToMany(technology, 'js', { [chain]: [value] })
      : []
  })
}

function analyzeDomSignals(
  signals,
  requiredTechnologyName,
  requiredCategoryId,
  explicitTechnologyNames
) {
  const technologies = getRequiredTechnologies(
    requiredTechnologyName,
    requiredCategoryId,
    explicitTechnologyNames
  )
  const technologiesByName = new Map(
    technologies.map((technology) => [technology.name, technology])
  )

  return signals.flatMap(
    ({ name, selector, exists, text, property, attribute, value }) => {
      const technology = technologiesByName.get(name)

      if (!technology) {
        return []
      }

      if (exists !== undefined) {
        return Wappalyzer.analyzeManyToMany(technology, 'dom.exists', {
          [selector]: [''],
        })
      }

      if (text !== undefined) {
        return Wappalyzer.analyzeManyToMany(technology, 'dom.text', {
          [selector]: [text],
        })
      }

      if (property !== undefined) {
        return Wappalyzer.analyzeManyToMany(
          technology,
          `dom.properties.${property}`,
          { [selector]: [value] }
        )
      }

      if (attribute !== undefined) {
        return Wappalyzer.analyzeManyToMany(
          technology,
          `dom.attributes.${attribute}`,
          { [selector]: [value] }
        )
      }

      return []
    }
  )
}

async function handleContentMessage(func, args, sender, pageToken) {
  const tabId = sender.tab?.id
  const frameId = sender.frameId || 0
  const messageUrl = isWebUrl(sender.url) ? sender.url : args[0]
  const state =
    typeof tabId === 'number' && tabId >= 0 && isWebUrl(messageUrl)
      ? getPageState(tabId, messageUrl, pageToken)
      : undefined

  if (state) {
    ensureAnalysisDeadline(tabId, state)
  }

  if (func === 'isDisabledDomain') {
    return false
  }

  if (func === 'analysisProgress') {
    if (state) {
      const [, stage, elapsedMs] = args
      const normalizedElapsedMs = Math.max(
        0,
        Number.isFinite(elapsedMs) ? Math.round(elapsedMs) : 0
      )

      state.currentStage = String(stage || 'unknown')
      state.stageTimingsMs[state.currentStage] = normalizedElapsedMs
    }

    return undefined
  }

  if (func === 'analysisFailed') {
    if (state) {
      const [, failureStage, errorMessage, stageTimingsMs = {}] = args

      finalizeAnalysis(tabId, state, {
        analysisStatus: 'failed',
        failureStage: String(failureStage || state.currentStage),
        errorMessage: String(errorMessage || 'Extension analysis failed'),
        stageTimingsMs,
      })
    }

    return undefined
  }

  if (func === 'analysisTimedOut') {
    if (state) {
      const [, failureStage, stageTimingsMs = {}] = args

      finalizeAnalysis(tabId, state, {
        analysisStatus: 'partial',
        failureStage: String(failureStage || state.currentStage),
        errorMessage: `Extension analysis exceeded ${ANALYSIS_DEADLINE_MS}ms`,
        stageTimingsMs,
      })
    }

    return undefined
  }

  await technologiesReady

  switch (func) {
    case 'getTechnologies':
      return Wappalyzer.technologies

    case 'onContentLoad': {
      const [
        url,
        originalItems = {},
        ,
        requiredTechnologyName,
        requiredCategoryId,
        options = {},
      ] = args
      const items = { ...originalItems }

      if (!options.skipCookies) {
        await addAccessibleCookies(url, items)
      }

      const analysisItems =
        options.includeUrl === false ? items : { url, ...items }
      const technologies = getRequiredTechnologies(
        requiredTechnologyName,
        requiredCategoryId,
        options.requiredTechnologyNames
      )

      recordDetections(
        tabId,
        frameId,
        url,
        Wappalyzer.analyze(analysisItems, technologies),
        pageToken
      )

      return undefined
    }

    case 'analyzeJs': {
      const [
        url,
        signals = [],
        requiredTechnologyName,
        requiredCategoryId,
        explicitTechnologyNames,
      ] = args

      recordDetections(
        tabId,
        frameId,
        url,
        analyzeJavaScriptSignals(
          signals,
          requiredTechnologyName,
          requiredCategoryId,
          explicitTechnologyNames
        ),
        pageToken
      )

      return undefined
    }

    case 'analyzeDom': {
      const [
        url,
        signals = [],
        requiredTechnologyName,
        requiredCategoryId,
        explicitTechnologyNames,
      ] = args

      recordDetections(
        tabId,
        frameId,
        url,
        analyzeDomSignals(
          signals,
          requiredTechnologyName,
          requiredCategoryId,
          explicitTechnologyNames
        ),
        pageToken
      )

      return undefined
    }

    case 'detectTechnology': {
      const [url, name] = args
      const technology = Wappalyzer.getTechnology(name)

      if (technology) {
        recordDetections(
          tabId,
          frameId,
          url,
          [
            {
              technology,
              pattern: { confidence: 100, type: 'manual', value: name },
              version: '',
            },
          ],
          pageToken
        )
      }

      return undefined
    }

    case 'analysisComplete': {
      const [, stageTimingsMs = {}] = args

      if (state) {
        finalizeAnalysis(tabId, state, {
          analysisStatus: 'complete',
          stageTimingsMs,
        })
      }

      return undefined
    }

    case 'error':
      console.error(...args)
      return undefined

    case 'log':
      return undefined

    default:
      throw new Error(`Unsupported content message: ${func}`)
  }
}

async function loadTechnologyDefinitions() {
  const categoriesResponse = await fetch(
    chrome.runtime.getURL('categories.json')
  )
  const technologyResponses = await Promise.all(
    ['_', ...'abcdefghijklmnopqrstuvwxyz'].map((letter) =>
      fetch(chrome.runtime.getURL(`technologies/${letter}.json`))
    )
  )
  const categories = await categoriesResponse.json()
  const technologyFiles = await Promise.all(
    technologyResponses.map((response) => response.json())
  )
  const technologies = Object.assign({}, ...technologyFiles)

  for (const technology of Object.values(technologies)) {
    delete technology.description
    delete technology.cpe
    delete technology.icon
    delete technology.pricing
    delete technology.website
  }

  Wappalyzer.setTechnologies(technologies)
  Wappalyzer.setCategories(categories)
}

async function loadRuntimeConfig() {
  const response = await fetch(chrome.runtime.getURL('runtime-config.json'))

  if (!response.ok) {
    throw new Error(`Could not load runtime config: HTTP ${response.status}`)
  }

  const { callback_url: callbackUrl } = await response.json()
  const parsed = new URL(callbackUrl)

  if (
    parsed.protocol !== 'http:' ||
    parsed.hostname !== '127.0.0.1' ||
    !parsed.port ||
    !parsed.pathname.startsWith('/technologies/')
  ) {
    throw new Error('runtime-config.json contains an invalid callback URL')
  }

  return { callbackUrl: parsed.toString() }
}

async function readExternalScriptSnippet(url) {
  const response = await fetch(url)

  if (!response.ok) {
    throw new Error(`Could not read script ${url}: HTTP ${response.status}`)
  }

  return (await response.text()).slice(0, MAX_EXTERNAL_SCRIPT_CHARACTERS)
}

async function analyzeCompletedExternalScript(request) {
  await technologiesReady

  const pageUrl = request.documentUrl || request.initiator

  if (
    request.tabId < 0 ||
    request.frameId > 0 ||
    !isWebUrl(pageUrl) ||
    !isWebUrl(request.url)
  ) {
    return
  }

  const state = getPageState(request.tabId, pageUrl)

  if (
    state.finalized ||
    state.analyzedScriptUrls.has(request.url) ||
    state.analyzedScriptUrls.size >= MAX_EXTERNAL_SCRIPTS_PER_PAGE
  ) {
    return
  }

  state.analyzedScriptUrls.add(request.url)

  try {
    const script = await readExternalScriptSnippet(request.url)
    const technologies = Wappalyzer.getTechnologiesByTypes(['scripts'])

    recordDetections(
      request.tabId,
      request.frameId,
      pageUrl,
      Wappalyzer.analyze({ scripts: script }, technologies)
    )
  } catch (error) {
    console.debug(error.message)
  }
}

async function analyzeCompletedRequestHost(request) {
  await technologiesReady

  const pageUrl = request.documentUrl || request.initiator

  if (
    request.tabId < 0 ||
    request.frameId > 0 ||
    !isWebUrl(pageUrl) ||
    !isWebUrl(request.url)
  ) {
    return
  }

  const requestHostname = new URL(request.url).hostname
  const state = getPageState(request.tabId, pageUrl)

  if (
    state.finalized ||
    state.analyzedRequestHosts.has(requestHostname) ||
    state.analyzedRequestHosts.size >= MAX_XHR_HOSTS_PER_PAGE
  ) {
    return
  }

  state.analyzedRequestHosts.add(requestHostname)
  const technologies = Wappalyzer.getTechnologiesByTypes(['xhr'])

  recordDetections(
    request.tabId,
    request.frameId,
    pageUrl,
    Wappalyzer.analyze({ xhr: requestHostname }, technologies)
  )
}

async function analyzeCompletedPageHeaders(request) {
  await technologiesReady

  if (request.tabId < 0 || !request.responseHeaders) {
    return
  }

  const headers = {}

  for (const { name, value, binaryValue } of request.responseHeaders) {
    const normalizedName = name?.toLowerCase()

    if (!normalizedName) {
      continue
    }

    headers[normalizedName] ||= []
    headers[normalizedName].push(String(value || binaryValue || ''))
  }

  const technologies = Wappalyzer.getTechnologiesByTypes(['headers'])

  recordDetections(
    request.tabId,
    0,
    request.url,
    Wappalyzer.analyze({ headers }, technologies)
  )
}

chrome.runtime.onMessage.addListener(
  ({ func, args = [], pageToken = '' }, sender, respond) => {
  if (!func) {
    return false
  }

  handleContentMessage(func, args, sender, pageToken)
    .then(respond)
    .catch((error) => {
      console.error(error)
      respond(undefined)
    })

  return true
  }
)

chrome.webRequest.onBeforeRequest.addListener(
  ({ tabId }) => deletePageState(tabId),
  { urls: ['http://*/*', 'https://*/*'], types: ['main_frame'] }
)

chrome.webRequest.onCompleted.addListener(
  (request) => void analyzeCompletedPageHeaders(request),
  { urls: ['http://*/*', 'https://*/*'], types: ['main_frame'] },
  ['responseHeaders']
)

chrome.webRequest.onCompleted.addListener(
  (request) => void analyzeCompletedExternalScript(request),
  { urls: ['http://*/*', 'https://*/*'], types: ['script'] }
)

chrome.webRequest.onCompleted.addListener(
  (request) => void analyzeCompletedRequestHost(request),
  { urls: ['http://*/*', 'https://*/*'], types: ['xmlhttprequest'] }
)

chrome.tabs.onRemoved.addListener(deletePageState)
