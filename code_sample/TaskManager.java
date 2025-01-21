// Java Example - Task Management System
package com.taskmanager.core;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Manages tasks and their statuses in the system.
 * Provides functionality for CRUD operations and task filtering.
 */
@Service
public class TaskManager implements TaskOperations {
    private static final int MAX_PRIORITY = 5;
    private final List<Task> tasks = new ArrayList<>();
    
    @Autowired
    private NotificationService notificationService;
    
    public enum TaskStatus {
        PENDING,
        IN_PROGRESS,
        COMPLETED,
        ARCHIVED
    }

    @Override
    public Optional<Task> createTask(String title, String description, int priority) {
        if (priority > MAX_PRIORITY) {
            throw new IllegalArgumentException("Priority cannot exceed " + MAX_PRIORITY);
        }

        Task newTask = Task.builder()
            .title(title)
            .description(description)
            .priority(priority)
            .status(TaskStatus.PENDING)
            .createdAt(LocalDateTime.now())
            .build();

        tasks.add(newTask);
        notificationService.sendNotification("New task created: " + title);
        return Optional.of(newTask);
    }

    public List<Task> getTasksByStatus(TaskStatus status) {
        return tasks.stream()
            .filter(task -> task.getStatus() == status)
            .sorted((t1, t2) -> t2.getPriority() - t1.getPriority())
            .collect(Collectors.toList());
    }
}