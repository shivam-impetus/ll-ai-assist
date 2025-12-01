// Global configuration and state
let config = null;

// Global state
let appState = {
    baseUrl : '',
    knowledgeBase: 'Leaplogic',
    source: 'Teradata',
    target: 'PySpark',
    file_filter: null,
    messages: [],
    feedbackLogs: [],
    processing: false,
    showReviewDashboard: false,
    filters: {
        feedback: 'All',
        framework: 'All'
    }
};

// DOM elements
let elements = {};

// Generate file filter based on current selections
function generateFileFilter() {
    if (appState.knowledgeBase === 'Leaplogic') {
        // For Leaplogic: include generic file + specific source-target combination
        const files = [`${appState.source.toLowerCase()}_generic.md`]; // Always include generic
        
        // Add specific source-target combination file
        const sourceTargetFile = `${appState.source.toLowerCase()}_to_${appState.target.toLowerCase()}.md`;
        files.push(sourceTargetFile);
        
        return files;
        
    } else if (appState.knowledgeBase === 'wm-python Framework') {
        // For wm-python Framework: include all .md files from common folder
        return "*";
        
    } else {
        console.log('Unknown knowledge base, using null filter');
        return null;
    }
}

// Update file filter when selections change
function updateFileFilter() {
    const newFilter = generateFileFilter();
    appState.file_filter = newFilter;
}

// Load configuration
async function loadConfig() {
    try {
        const response = await fetch('./config.json');
        config = await response.json();
        
        // Construct baseUrl from protocol, host, and port
        appState.baseUrl = `${config.api.protocol}://${config.api.host}:${config.api.port}`;
        
        // Update appState with config defaults
        appState.knowledgeBase = config.ui.defaultKnowledgeBase;
        appState.source = config.ui.defaultSource;
        appState.target = config.ui.defaultTarget;
    } catch (error) {
        console.error('Failed to load config.', error);
    }
}

// Initialize the application
document.addEventListener('DOMContentLoaded', async function() {
    console.log('DOM loaded, initializing application...');
    await loadConfig();
    initializeElements();
    initializeEventListeners();
    await updateUI();
    enableChatInput();
    console.log('Application initialized successfully');
});

// Initialize DOM element references
function initializeElements() {
    elements = {
        sidebar: document.getElementById('sidebar'),
        sidebarToggle: document.getElementById('sidebarToggle'),
        knowledgeBase: document.getElementById('knowledgeBase'),
        source: document.getElementById('source'),
        target: document.getElementById('target'),
        leaplogicOptions: document.getElementById('leaplogicOptions'),
        docInfo: document.getElementById('docInfo'),
        modelName: document.getElementById('modelName'),
        messageCount: document.getElementById('messageCount'),
        chatInput: document.getElementById('chatInput'),
        sendBtn: document.getElementById('sendBtn'),
        chatMessages: document.getElementById('chatMessages'),
        welcomeMessage: document.getElementById('welcomeMessage'),
        clearChatBtn: document.getElementById('clearChatBtn'),
        reloadDbBtn: document.getElementById('reloadDbBtn'),
        viewLoggedQuestionsBtn: document.getElementById('viewLoggedQuestionsBtn'),
        reviewDashboard: document.getElementById('reviewDashboard'),
        mainContent: document.getElementById('mainContent'),
        backToChatBtn: document.getElementById('backToChatBtn'),
        loadingOverlay: document.getElementById('loadingOverlay'),
        toastContainer: document.getElementById('toastContainer'),
        feedbackFilter: document.getElementById('feedbackFilter'),
        frameworkFilter: document.getElementById('frameworkFilter'),
        exportCsvBtn: document.getElementById('exportCsvBtn'),
        feedbackEntries: document.getElementById('feedbackEntries'),
        totalQuestions: document.getElementById('totalQuestions'),
        helpfulCount: document.getElementById('helpfulCount'),
        notHelpfulCount: document.getElementById('notHelpfulCount'),
        notMarkedCount: document.getElementById('notMarkedCount')
    };
}

// Initialize event listeners
function initializeEventListeners() {
    // Sidebar toggle
    elements.sidebarToggle.addEventListener('click', toggleSidebar);
    
    // Knowledge base selector
    elements.knowledgeBase.addEventListener('change', handleKnowledgeBaseChange);
    elements.source.addEventListener('change', handleSourceChange);
    elements.target.addEventListener('change', handleTargetChange);
    
    // Chat input
    elements.chatInput.addEventListener('keypress', handleChatInputKeypress);
    elements.sendBtn.addEventListener('click', handleSendMessage);
    
    // Action buttons
    elements.clearChatBtn.addEventListener('click', handleClearChat);
    elements.reloadDbBtn.addEventListener('click', handleReloadDb);
    elements.viewLoggedQuestionsBtn.addEventListener('click', showReviewDashboard);
    elements.backToChatBtn.addEventListener('click', hideReviewDashboard);
    
    // Review dashboard
    elements.feedbackFilter.addEventListener('change', updateFeedbackEntries);
    elements.frameworkFilter.addEventListener('change', updateFeedbackEntries);
    elements.exportCsvBtn.addEventListener('click', exportToCsv);
    
    // Example question buttons
    document.addEventListener('click', handleExampleQuestionClick);
    
    // Responsive sidebar
    window.addEventListener('resize', handleWindowResize);
}

// Toggle sidebar
function toggleSidebar() {
    elements.sidebar.classList.toggle('collapsed');
    if (window.innerWidth <= 768) {
        elements.sidebar.classList.toggle('show');
    }
}

// Handle knowledge base change
async function handleKnowledgeBaseChange() {
    appState.knowledgeBase = elements.knowledgeBase.value; // Debug log
    await updateUI();
    updateExampleQuestions();
    showToast(`Switched to ${appState.knowledgeBase} knowledge base`);
}

// Handle source change
function handleSourceChange() {
    appState.source = elements.source.value;
    showToast(`Source updated to ${appState.source}`);
}

// Handle target change
function handleTargetChange() {
    appState.target = elements.target.value;
    showToast(`Target updated to ${appState.target}`);
}

// Handle chat input keypress
function handleChatInputKeypress(e) {
    if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        handleSendMessage();
    }
}

// Handle send message
function handleSendMessage() {
    const message = elements.chatInput.value.trim();
    if (!message || appState.processing) {
        console.log('Message empty or processing, returning');
        return;
    }
    
    processUserQuestion(message);
    elements.chatInput.value = '';
}

// Handle example question click
function handleExampleQuestionClick(e) {
    if (e.target.classList.contains('example-btn') && !appState.processing) {
        const question = e.target.dataset.question;
        if (question) {
            processUserQuestion(question);
        }
    }
}

// Handle clear chat
function handleClearChat() {
    if (appState.processing) return;
    
    appState.messages = [];
    updateChatDisplay();
    updateMessageCount();
    showToast('Chat history cleared!');
}

// Handle reload database
async function handleReloadDb() {
    if (appState.processing) return;
    
    const reloadKnowledgeBaseApiUrl = `${appState.baseUrl}${config.api.endpoints.reloadKnowledgeBase}`;
    const response = await fetch(reloadKnowledgeBaseApiUrl, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        mode: 'cors'
    });

    const data = await response.json();

    setTimeout(() => {
        hideLoading();
        showToast(data.message);
    }, 2000);
}

// Handle window resize
function handleWindowResize() {
    if (window.innerWidth > 768) {
        elements.sidebar.classList.remove('show');
    }
}

// Process user question
function processUserQuestion(question) {
    if (appState.processing) {
        console.log('Already processing, returning');
        return;
    }
    
    appState.processing = true;
    disableChatInput();
    
    // Add user message
    const userMessage = {
        role: 'user',
        content: question,
        timestamp: new Date().toISOString()
    };
    
    appState.messages.push(userMessage);
    updateChatDisplay();
    hideWelcomeMessage();
    
    // Show loading
    showLoading();
    
    // Call backend API
    generateAIResponse(question);
}


// Generate AI response
async function generateAIResponse(question) {
    console.log('generateAIResponse called with question:', question);
    
    // Update file filter based on current selections before making API call
    updateFileFilter();
    
    try {
        // Prepare the request payload
        console.log('All messages in appState:', appState.messages);
        console.log('Total messages count:', appState.messages.length);
        
        // Get previous messages (excluding current user message which is the last one)
        const previousMessages = appState.messages.slice(0, -1); // Exclude current user message
        const recentMessages = previousMessages.slice(-6); // Get last 6 previous messages (3 exchanges)
        const conversationHistory = [];
        
        console.log('Previous messages for context:', previousMessages.length);
        console.log('Recent messages for context:', recentMessages);
        
        // Group messages into question-answer pairs
        for (let i = 0; i < recentMessages.length - 1; i += 2) {
            console.log(`Checking pair at index ${i}:`, recentMessages[i], recentMessages[i + 1]);
            if (recentMessages[i] && recentMessages[i].role === 'user' && 
                recentMessages[i + 1] && recentMessages[i + 1].role === 'assistant') {
                const pair = {
                    question: recentMessages[i].content,
                    answer: recentMessages[i + 1].content
                };
                console.log('Adding conversation pair:', pair);
                conversationHistory.push(pair);
            }
        }
                
        const payload = {
            question: question,
            file_filter: appState.file_filter,
            conversation_history: conversationHistory
        };
        
        console.log('Sending payload:', payload);

        // Call the backend API - try original port 8000 first
        const generateAnswerApiUrl = `${appState.baseUrl}${config.api.endpoints.generateAnswer}`;
        const response = await fetch(generateAnswerApiUrl, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            mode: 'cors',
            body: JSON.stringify(payload)
        });
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        console.log('Response data:', data);
        
        // Process the response data
        const sources = data.search_results || [];
        
        // Add assistant message
        const assistantMessage = {
            role: 'assistant',
            content: data.answer,
            sources: sources,
            timestamp: data.timestamp || new Date().toISOString(),
            feedbackGiven: false
        };
        
        appState.messages.push(assistantMessage);
        
        // Log the interaction
        logFeedback(question, data.answer, 'Not Marked', sources);
        
        hideLoading();
        updateChatDisplay();
        enableChatInput();
        appState.processing = false;
        
    } catch (error) {
        console.error('Error calling backend API:', error);
        
        // Fallback to dummy data on error
        handleApiError(question, error);
        
        hideLoading();
        enableChatInput();
        appState.processing = false;
    }
}

// Handle API errors with fallback response
function handleApiError(question, error) {
    const errorMessage = {
        role: 'assistant',
        content: `I apologize, but I'm having trouble connecting to the knowledge base right now. Please try again in a moment.\n\nError: ${error.message}`,
        sources: [],
        timestamp: new Date().toISOString(),
        feedbackGiven: false
    };
    
    appState.messages.push(errorMessage);
    updateChatDisplay();
    showToast('Connection error. Please try again.', 'error');
}

// Generate generic response for questions not in the data
function generateGenericResponse(question, knowledgeBase) {
    const genericResponses = {
        leaplogic: {
            answer: `Thank you for your question about "${question}". This appears to be related to Leaplogic migration from ${appState.source} to ${appState.target}. \n\nFor specific technical details about this topic, I'd recommend checking the detailed migration documentation. The conversion process typically involves analyzing the source code structure and applying appropriate transformation patterns.\n\nIf you have a more specific question about syntax conversion, data type mapping, or function equivalents, please feel free to ask!`,
            sources: []
        },
        common: {
            answer: `Thank you for asking about "${question}". This relates to the WMG framework capabilities.\n\nThe framework provides comprehensive utilities for data processing, migration, and management across various platforms. For specific implementation details, you might want to explore the relevant utility modules.\n\nCould you provide more details about what specific aspect you'd like to know more about? For example, are you interested in ETL processes, data validation, or cloud migration features?`,
            sources: []
        }
    };
    
    return genericResponses[knowledgeBase];
}

// Log feedback
function logFeedback(question, answer, feedback, sources = [], framework = null, source = null, target = null) {
    // Handle different source formats (API response vs fallback data)
    let sourcesList = '';
    if (Array.isArray(sources)) {
        if (sources.length > 0 && typeof sources[0] === 'object' && sources[0].file) {
            // API format: [{file: "name.md", confidence: 0.9}]
            sourcesList = sources.map(s => s.file).join('; ');
        } else if (sources.length > 0 && typeof sources[0] === 'string') {
            // Fallback format: ["name.md", "name2.md"]
            sourcesList = sources.join('; ');
        }
    }
    
    const logEntry = {
        id: appState.feedbackLogs.length + 1,
        question: question,
        answer: answer,
        feedback: feedback,
        sources: sourcesList,
        framework: framework || appState.knowledgeBase,
        source: appState.knowledgeBase === 'Leaplogic' ? appState.source : '',
        target: appState.knowledgeBase === 'Leaplogic' ? appState.target : '',
        timestamp: new Date().toISOString()
    };
    
    appState.feedbackLogs.push(logEntry);
    updateFeedbackStats();
}

// Update feedback for existing entry
function updateFeedback(messageIndex, feedback) {
    if (messageIndex >= 0 && messageIndex < appState.messages.length) {
        const message = appState.messages[messageIndex];
        if (message.role === 'assistant') {
            message.feedbackGiven = true;
            message.feedback = feedback;
            
            // Find and update the corresponding log entry
            const userMessage = appState.messages[messageIndex - 1];
            if (userMessage) {
                const logEntry = appState.feedbackLogs.find(log => 
                    log.question === userMessage.content && 
                    log.answer === message.content
                );
                
                if (logEntry) {
                    logEntry.feedback = feedback;
                }
            }
            
            updateChatDisplay();
            updateFeedbackStats();
            showToast(feedback === 'helpful' ? 'Thanks for your feedback!' : 'Feedback logged. We\'ll improve this answer!');
        }
    }
}

// Update UI based on current state
async function updateUI() {
    // Update Leaplogic options visibility
    if (appState.knowledgeBase === 'Leaplogic') {
        elements.leaplogicOptions.style.display = 'block';
    } else {
        elements.leaplogicOptions.style.display = 'none';
    }
    
    try {
        const modelNameApiUrl = `${appState.baseUrl}${config.api.endpoints.getModelName}`;
        const response = await fetch(modelNameApiUrl, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
            }
        });
        
        if (response.ok) {
            const modelData = await response.json();
            elements.modelName.textContent = modelData.model_name;
        } else {
            console.warn('Failed to fetch model name.');
            elements.modelName.textContent = "";
        }
    } catch (error) {
        console.error('Error fetching model name.', error);
        elements.modelName.textContent = "";
    }


    // Fetch and update statistics info from backend
    try {
        const getStatisticsApiUrl = `${appState.baseUrl}${config.api.endpoints.getStatistics}`;
        const response = await fetch(getStatisticsApiUrl, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
            }
        });
        
        if (response.ok) {
            const statsData = await response.json();
            elements.docInfo.textContent = `${statsData.documents_loaded} files, ${statsData.total_chunks} chunks`;
        } else {
            console.warn('Failed to fetch statistics.');
        }
    } catch (error) {
        console.error('Error fetching statistics.', error);
    }
    
    updateMessageCount();
    updateExampleQuestions();
}

// Update example questions based on knowledge base
function updateExampleQuestions() {
    const kbKey = appState.knowledgeBase === 'Leaplogic' ? 'leaplogic' : 'common';
    const questions = kbKey === 'leaplogic' ? [
        { text: 'Date functions', question: 'What are the equivalent date functions in PySpark for Teradata?' },
        { text: 'Qualify Clause Conversion', question: 'How is qualify clause converted?' }
    ] : [
        { text: 'Execute Query', question: 'How to execute query on glue?' },
        { text: 'DDL Comparison', question: 'How to compare ddls?' }
    ];
    
    const leaplogicExamples = document.querySelectorAll('.leaplogic-example');
    const commonExamples = document.querySelectorAll('.common-example');
        
    if (appState.knowledgeBase === 'Leaplogic') {
        leaplogicExamples.forEach((btn, index) => {
            btn.style.display = 'block';
            if (questions[index]) {
                btn.textContent = questions[index].text;
                btn.dataset.question = questions[index].question;
            }
        });
        commonExamples.forEach(btn => btn.style.display = 'none');
    } else {
        leaplogicExamples.forEach(btn => btn.style.display = 'none');
        commonExamples.forEach((btn, index) => {
            btn.style.display = 'block';
            if (questions[index]) {
                btn.textContent = questions[index].text;
                btn.dataset.question = questions[index].question;
            }
        });
    }
}

// Update message count
function updateMessageCount() {
    elements.messageCount.textContent = appState.messages.length;
}

// Update chat display
function updateChatDisplay() {
    elements.chatMessages.innerHTML = '';
    
    appState.messages.forEach((message, index) => {
        const messageDiv = createMessageElement(message, index);
        elements.chatMessages.appendChild(messageDiv);
    });
    
    // Scroll to bottom
    elements.chatMessages.scrollTop = elements.chatMessages.scrollHeight;
    updateMessageCount();
}

// Create message element
function createMessageElement(message, index) {
    const messageDiv = document.createElement('div');
    messageDiv.className = `chat-message ${message.role}`;
    
    const avatar = message.role === 'user' ? '👤' : '🤖';
    
    let sourcesHtml = '';
    if (message.sources && message.sources.length > 0) {
        let sourcesList = '';
        
        // Handle different source formats
        if (typeof message.sources[0] === 'object' && message.sources[0].file) {
            // API format: [{file: "name.md", confidence: 0.9}]
            sourcesList = message.sources.map(source => 
                `<li><strong>${source.file}</strong> (confidence: ${(source.confidence * 100).toFixed(0)}%)</li>`
            ).join('');
        } else if (typeof message.sources[0] === 'string') {
            // Fallback format: ["name.md", "name2.md"] 
            sourcesList = message.sources.map(source => 
                `<li><strong>${source}</strong></li>`
            ).join('');
        }
        
        if (sourcesList) {
            sourcesHtml = `
                <details class="sources-expandable">
                    <summary>📚 View Sources</summary>
                    <div class="sources-content">
                        <ul>${sourcesList}</ul>
                    </div>
                </details>
            `;
        }
    }
    
    let feedbackHtml = '';
    if (message.role === 'assistant') {
        if (!message.feedbackGiven) {
            feedbackHtml = `
                <div class="feedback-buttons">
                    <button class="feedback-btn helpful" onclick="updateFeedback(${index}, 'helpful')">👍</button>
                    <button class="feedback-btn not-helpful" onclick="updateFeedback(${index}, 'not helpful')">👎</button>
                </div>
            `;
        } else {
            const feedbackText = message.feedback === 'helpful' ? '✓ Marked as helpful' : 
                                message.feedback === 'not helpful' ? '⚠️ Marked for improvement' : 
                                '○ Not marked';
            feedbackHtml = `<div class="feedback-given">${feedbackText}</div>`;
        }
    }
    
    messageDiv.innerHTML = `
        <div class="message-avatar">${avatar}</div>
        <div class="message-content">
            <div class="message-text">${formatMessageContent(message.content)}</div>
            ${sourcesHtml}
            ${feedbackHtml}
        </div>
    `;
    
    return messageDiv;
}

// Format message content (basic markdown support)
function formatMessageContent(content) {
    return content
        .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
        .replace(/\*(.*?)\*/g, '<em>$1</em>')
        .replace(/```(\w+)?\n([\s\S]*?)```/g, '<pre><code>$2</code></pre>')
        .replace(/`(.*?)`/g, '<code>$1</code>')
        .replace(/\n/g, '<br>');
}

// Show/hide welcome message
function hideWelcomeMessage() {
    if (appState.messages.length > 0) {
        elements.welcomeMessage.style.display = 'none';
    }
}

function showWelcomeMessage() {
    if (appState.messages.length === 0) {
        elements.welcomeMessage.style.display = 'block';
    }
}

// Enable/disable chat input
function enableChatInput() {
    if (elements.chatInput) {
        elements.chatInput.disabled = false;
        console.log('Chat input enabled');
    } else {
        console.error('Chat input element not found!');
    }
    if (elements.sendBtn) {
        elements.sendBtn.disabled = false;
    } else {
        console.error('Send button element not found!');
    }
    if (elements.chatInput) {
        elements.chatInput.placeholder = '💬 Ask me anything about the docs...';
    }
}

function disableChatInput() {
    elements.chatInput.disabled = true;
    elements.sendBtn.disabled = true;
    elements.chatInput.placeholder = 'Processing...';
}

// Show/hide loading
function showLoading() {
    elements.loadingOverlay.style.display = 'flex';
}

function hideLoading() {
    elements.loadingOverlay.style.display = 'none';
}

// Show toast notification
function showToast(message, type = 'success') {
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.textContent = message;
    
    elements.toastContainer.appendChild(toast);
    
    // Remove toast after 3 seconds
    setTimeout(() => {
        if (toast.parentNode) {
            toast.parentNode.removeChild(toast);
        }
    }, 3000);
}

// Review Dashboard Functions
function showReviewDashboard() {
    appState.showReviewDashboard = true;
    elements.reviewDashboard.style.display = 'block';
    elements.mainContent.style.display = 'none';
    updateFeedbackStats();
    updateFeedbackEntries();
}

function hideReviewDashboard() {
    appState.showReviewDashboard = false;
    elements.reviewDashboard.style.display = 'none';
    elements.mainContent.style.display = 'flex';
}

// Update feedback statistics
function updateFeedbackStats() {
    const stats = calculateFeedbackStats();
    elements.totalQuestions.textContent = stats.total;
    elements.helpfulCount.textContent = stats.helpful;
    elements.notHelpfulCount.textContent = stats.notHelpful;
    elements.notMarkedCount.textContent = stats.notMarked;
}

// Calculate feedback statistics
function calculateFeedbackStats() {
    const total = appState.feedbackLogs.length;
    const helpful = appState.feedbackLogs.filter(log => log.feedback === 'helpful').length;
    const notHelpful = appState.feedbackLogs.filter(log => log.feedback === 'not helpful').length;
    const notMarked = appState.feedbackLogs.filter(log => log.feedback === 'Not Marked').length;
    
    return { total, helpful, notHelpful, notMarked };
}

// Update feedback entries display
function updateFeedbackEntries() {
    const feedbackFilter = elements.feedbackFilter.value;
    const frameworkFilter = elements.frameworkFilter.value;
    
    let filteredLogs = appState.feedbackLogs;
    
    // Apply feedback filter
    if (feedbackFilter !== 'All') {
        filteredLogs = filteredLogs.filter(log => {
            if (feedbackFilter === 'Helpful') return log.feedback === 'helpful';
            if (feedbackFilter === 'Not Helpful') return log.feedback === 'not helpful';
            if (feedbackFilter === 'Not Marked') return log.feedback === 'Not Marked';
            return true;
        });
    }
    
    // Apply framework filter
    if (frameworkFilter !== 'All') {
        filteredLogs = filteredLogs.filter(log => log.framework === frameworkFilter);
    }
    
    // Update entries title
    const entriesTitle = elements.feedbackEntries.querySelector('h3');
    entriesTitle.textContent = `📝 Feedback Entries (${filteredLogs.length})`;
    
    // Clear existing entries
    const existingEntries = elements.feedbackEntries.querySelectorAll('.feedback-entry');
    existingEntries.forEach(entry => entry.remove());
    
    // Add filtered entries
    filteredLogs.reverse().forEach((log, index) => {
        const entryElement = createFeedbackEntry(log, index);
        elements.feedbackEntries.appendChild(entryElement);
    });
    
    // Show message if no entries
    if (filteredLogs.length === 0) {
        const noEntriesDiv = document.createElement('div');
        noEntriesDiv.className = 'no-entries';
        noEntriesDiv.innerHTML = '<p style="text-align: center; color: #6b7280; padding: 2rem;">No feedback entries found for current filters.</p>';
        elements.feedbackEntries.appendChild(noEntriesDiv);
    }
}

// Create feedback entry element
function createFeedbackEntry(log, index) {
    const entryDiv = document.createElement('div');
    entryDiv.className = 'feedback-entry';
    
    const emoji = log.feedback === 'helpful' ? '👍' : 
                  log.feedback === 'not helpful' ? '👎' : '○';
    
    const color = log.feedback === 'helpful' ? '#10b981' : 
                  log.feedback === 'not helpful' ? '#ef4444' : '#9ca3af';
    
    const questionPreview = log.question.length > 80 ? 
                           log.question.substring(0, 80) + '...' : 
                           log.question;
    
    entryDiv.innerHTML = `
        <div class="feedback-entry-header" onclick="toggleFeedbackEntry(${index})">
            <span style="color: ${color}">${emoji}</span>
            <span>${questionPreview}</span>
        </div>
        <div class="feedback-entry-content">
            <div class="feedback-entry-question">
                <strong>❓ Question:</strong> ${log.question}
            </div>
            <div class="feedback-entry-answer">
                <strong>🤖 Answer:</strong> ${log.answer}
            </div>
            <div class="feedback-entry-meta">
                <div>
                    <strong>📝 Feedback:</strong> 
                    <span style="color: ${color}; font-weight: bold;">${log.feedback}</span>
                </div>
                <div>
                    <strong>🔧 Framework:</strong> ${log.framework}
                </div>
                ${log.sources ? `<div class="feedback-entry-sources"><strong>📚 Sources:</strong> ${log.sources}</div>` : ''}
                ${log.framework === 'Leaplogic' ? 
                  `<div><strong>📥 Source:</strong> ${log.source} | <strong>Target:</strong> ${log.target}</div>` : ''}
            </div>
        </div>
    `;
    
    return entryDiv;
}

// Toggle feedback entry expansion
function toggleFeedbackEntry(index) {
    const entries = document.querySelectorAll('.feedback-entry');
    if (entries[index]) {
        entries[index].classList.toggle('expanded');
    }
}

// Export to CSV
function exportToCsv() {
    const headers = ['Framework', 'Source', 'Target', 'Question', 'Answer', 'Feedback', 'Documentation Sources', 'Timestamp'];
    
    const csvContent = [
        headers.join(','),
        ...appState.feedbackLogs.map(log => [
            `"${log.framework}"`,
            `"${log.source}"`,
            `"${log.target}"`,
            `"${log.question.replace(/"/g, '""')}"`,
            `"${log.answer.replace(/"/g, '""')}"`,
            `"${log.feedback}"`,
            `"${log.sources}"`,
            `"${log.timestamp}"`
        ].join(','))
    ].join('\n');
    
    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `feedback_log_${new Date().toISOString().split('T')[0]}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    window.URL.revokeObjectURL(url);
    
    showToast('CSV exported successfully!');
}

// Initialize feedback stats on load
document.addEventListener('DOMContentLoaded', function() {
    setTimeout(updateFeedbackStats, 100);
});