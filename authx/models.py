from django.db import models
from django.contrib.auth.models import User
from django.db.models.signals import post_save
from django.dispatch import receiver


class UserProfile(models.Model):
    """
    User profile to store preferences from Check User Access API
    """
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='profile', db_index=True)
    language = models.CharField(max_length=10, default='EN', help_text='User language preference (e.g., EN, ES)', db_index=False)
    unit_system = models.CharField(max_length=20, default='Imperial', help_text='Unit system preference (Imperial/Metric)', db_index=False)
    preferred_mode = models.CharField(max_length=10, default='Light', help_text='Theme preference (Light/Dark)', db_index=False)

    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'authx_userprofile'
        verbose_name = 'User Profile'
        verbose_name_plural = 'User Profiles'

    def __str__(self):
        return f"{self.user.username} Profile"


@receiver(post_save, sender=User)
def create_user_profile(sender, instance, created, **kwargs):
    """
    Automatically create a UserProfile when a User is created
    """
    if created:
        UserProfile.objects.create(user=instance)


@receiver(post_save, sender=User)
def save_user_profile(sender, instance, **kwargs):
    """
    Automatically save the UserProfile when User is saved
    """
    if hasattr(instance, 'profile'):
        instance.profile.save()
